package bqcpp;


import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.types.DataTypes;

import java.util.Collection;
import java.util.Comparator;
import java.util.stream.Collectors;

import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.functions.col;

public class BqcppSolver {

    // number of files in the data lake
    public static final long F = 10_000;

    // number of records in the data lake
    public static final long N = 5_999_989_709L;

    // used in files estimation
    public static final double FACTOR = (F - 1) / (F * 1.0);

    // for each clause assign estimated cost (number of indexes to read from the storage) and estimated result (number of records) based on root-index statistics
    public static void assignEstimations(Collection<Clause> clauses, Dataset rootIndex){

        for (Clause c : clauses){
            switch(c.type){

                case LESS_OR_EQUAL_THAN:

                    c.cost = rootIndex
                            .where(col("col").equalTo(c.columnName).and(col("min").leq(lit(Integer.valueOf(c.columnValue1)))))
                            .select("file").distinct().count();

                    c.result = rootIndex
                        .where(col("col").equalTo(c.columnName).and(col("min").leq(lit(Integer.valueOf(c.columnValue1)))))
                        .withColumn("my_cnt",
                                when(col("max").cast(DataTypes.LongType).leq(lit(Long.valueOf(c.columnValue1))), col("cnt"))
                                        .otherwise((lit(Long.valueOf(c.columnValue1)).minus(col("min").cast(DataTypes.LongType)))
                                                .divide(col("max").cast(DataTypes.LongType).minus(col("min").cast(DataTypes.LongType)))
                                                .multiply(col("cnt"))).cast(DataTypes.LongType))
                        .groupBy().sum("my_cnt").as(Encoders.LONG()).collectAsList().get(0);
                    break;

                case RANGE:

                    c.cost = rootIndex
                            .where(col("col").equalTo(c.columnName).and(not(col("max").lt(lit(c.columnValue1)).or(col("min").gt(lit(c.columnValue2))))))
                            .select("file").distinct().count();

                    c.result = rootIndex
                            .where(col("col").equalTo(c.columnName).and(not(col("max").lt(lit(c.columnValue1)).or(col("min").gt(lit(c.columnValue2))))))
                            .withColumn("my_cnt",
                                    when(col("min").geq(c.columnValue1).and(col("max").leq(c.columnValue2)), col("cnt"))
                                            .otherwise(when(col("min").lt(c.columnValue1), datediff(lit(c.columnValue1), col("min"))
                                                    .divide(datediff(col("max"), col("min")))
                                                            .multiply(col("cnt")).cast(DataTypes.LongType))
                                                    .otherwise(
                                                            datediff(col("max"), lit(c.columnValue2))
                                                                    .divide(datediff(col("max"), col("min")))
                                                                            .multiply(col("cnt")).cast(DataTypes.LongType)))
                            ).groupBy().sum("my_cnt").as(Encoders.LONG()).collectAsList().get(0);

                    break;

                //TODO add handling for the rest of the clause types

            }
        }

    }

    // greedy algorithm to compute a balanced plan - we sort the clauses by the total cost and add clauses from the bottom until the total cost keep reducing
    // in our case (when the result is estimated by the number of records) - this algorithm returns the optimal solution
    public static Plan getBalancedPlanByGreedyAlgorithm(Collection<Clause> clauses){

        Plan result = new Plan();

        for (Clause c : clauses.stream().sorted(Comparator.comparing(Clause::getTotalCost)).collect(Collectors.toList())){
            if (result.clauses.isEmpty()){
                result.clauses.add(c);
                result.cost = c.cost;
            }else if (result.getExpectedTotalCost(c) < result.getTotalCost()){
                result.clauses.add(c);
                result.cost += c.cost;
            }else{
                break;
            }
        }

        return result;

    }

}
