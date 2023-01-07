package bqcpp;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.junit.jupiter.api.Test;
import testutils.MyUtils;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import static bqcpp.ClauseType.LESS_OR_EQUAL_THAN;
import static bqcpp.ClauseType.RANGE;
import static org.apache.spark.sql.functions.*;
import static tpch.Benchmark.*;

public class BqcppSolverTest {

    List<Clause> q1 = Arrays.asList(
            new Clause(LESS_OR_EQUAL_THAN, "l_extendedprice", "969", null),
            new Clause(RANGE, "l_shipdate", "1994-01-06", "1994-01-10"),
            new Clause(RANGE, "l_commitdate", "1994-01-01", "1994-01-05")
    );

    List<Clause> q2 = Arrays.asList(
            new Clause(LESS_OR_EQUAL_THAN, "l_extendedprice", "972", null),
            new Clause(RANGE, "l_shipdate", "1994-01-10", "1994-01-20"),
            new Clause(RANGE, "l_commitdate", "1994-01-01", "1994-01-09")
    );

    List<Clause> q3 = Arrays.asList(
            new Clause(LESS_OR_EQUAL_THAN, "l_extendedprice", "980", null),
            new Clause(RANGE, "l_shipdate", "1994-01-16", "1994-01-26"),
            new Clause(RANGE, "l_commitdate", "1994-01-01", "1994-01-15")
    );

    List<Clause> q4 = Arrays.asList(
            new Clause(LESS_OR_EQUAL_THAN, "l_extendedprice", "1001", null),
            new Clause(RANGE, "l_shipdate", "1994-01-20", "1994-01-30"),
            new Clause(RANGE, "l_commitdate", "1994-01-01", "1994-01-19")
    );

    List<Clause> q5 = Arrays.asList(
            new Clause(LESS_OR_EQUAL_THAN, "l_extendedprice", "1030", null),
            new Clause(RANGE, "l_shipdate", "1994-01-24", "1994-02-01"),
            new Clause(RANGE, "l_commitdate", "1994-01-01", "1994-01-23")
    );

    List<Clause> q6 = Arrays.asList(
            new Clause(LESS_OR_EQUAL_THAN, "l_extendedprice", "1050", null),
            new Clause(RANGE, "l_shipdate", "1994-01-26", "1994-02-03"),
            new Clause(RANGE, "l_commitdate", "1994-01-01", "1994-01-25")
    );

    List<Clause> q7 = Arrays.asList(
            new Clause(LESS_OR_EQUAL_THAN, "l_extendedprice", "1079", null),
            new Clause(RANGE, "l_shipdate", "1994-01-31", "1994-02-06"),
            new Clause(RANGE, "l_commitdate", "1994-01-01", "1994-01-30")
    );

    List<Clause> q8 = Arrays.asList(
            new Clause(LESS_OR_EQUAL_THAN, "l_extendedprice", "1092", null),
            new Clause(RANGE, "l_shipdate", "1994-01-31", "1994-02-07"),
            new Clause(RANGE, "l_commitdate", "1994-01-01", "1994-01-30")
    );

    List<Clause> q9 = Arrays.asList(
            new Clause(LESS_OR_EQUAL_THAN, "l_extendedprice", "1098", null),
            new Clause(RANGE, "l_shipdate", "1994-01-31", "1994-02-09"),
            new Clause(RANGE, "l_commitdate", "1994-01-01", "1994-01-30")
    );

    List<Clause> q10 = Arrays.asList(
            new Clause(LESS_OR_EQUAL_THAN, "l_extendedprice", "1105", null),
            new Clause(RANGE, "l_shipdate", "1994-02-01", "1994-02-12"),
            new Clause(RANGE, "l_commitdate", "1994-01-01", "1994-01-31")
    );

    List<Clause> q11 = Arrays.asList(
            new Clause(LESS_OR_EQUAL_THAN, "l_extendedprice", "1115", null),
            new Clause(RANGE, "l_shipdate", "1994-02-01", "1994-02-16"),
            new Clause(RANGE, "l_commitdate", "1994-01-01", "1994-01-31")
    );

    List<Clause> q12 = Arrays.asList(
            new Clause(LESS_OR_EQUAL_THAN, "l_extendedprice", "1122", null),
            new Clause(RANGE, "l_shipdate", "1994-02-01", "1994-02-20"),
            new Clause(RANGE, "l_commitdate", "1994-01-01", "1994-01-31")
    );

    List<Clause> q13 = Arrays.asList(
            new Clause(LESS_OR_EQUAL_THAN, "l_extendedprice", "1137", null),
            new Clause(RANGE, "l_shipdate", "1994-02-01", "1994-03-01"),
            new Clause(RANGE, "l_commitdate", "1994-01-01", "1994-01-31")
    );

    List<Clause> q14 = Arrays.asList(
            new Clause(LESS_OR_EQUAL_THAN, "l_extendedprice", "2000", null),
            new Clause(RANGE, "l_shipdate", "1994-02-01", "1994-03-30"),
            new Clause(RANGE, "l_commitdate", "1994-01-01", "1994-01-31")
    );

    @Test
    public void estimationsForQueriesTest(){
        SparkSession sparkSession = MyUtils.initTestSparkSession("estimationsForQueriesTest");

        Dataset rootIndex = sparkSession.read().json("src/test/resources/root-index/root.json");

        String [][] queries = {queryInput1, queryInput2, queryInput3, queryInput4, queryInput5, queryInput6, queryInput7, queryInput8,
                queryInput9, queryInput10, queryInput11, queryInput12, queryInput13, queryInput14};


        final long N = 5999989709L;

        final long F = 10000;

        final double factor = 0.9999; // (F -1) / F

        List<List <String>> results = new LinkedList<>();
        for (String [] q : queries) {

            long val1 = rootIndex
                    .where(col("col").equalTo("l_extendedprice").and(col("min").leq(lit(Integer.valueOf(q[1])))))
                    .withColumn("my_cnt",
                            when(col("max").cast(DataTypes.LongType).leq(lit(Long.valueOf(q[1]))), col("cnt"))
                                    .otherwise((lit(Long.valueOf(q[1])).minus(col("min").cast(DataTypes.LongType)))
                                            .divide(col("max").cast(DataTypes.LongType).minus(col("min").cast(DataTypes.LongType)))
                                            .multiply(col("cnt"))).cast(DataTypes.LongType))
                    .groupBy().sum("my_cnt").as(Encoders.LONG()).collectAsList().get(0);

            long val2 = rootIndex
                    .where(col("col").equalTo("l_shipdate").and(not(col("max").lt(lit(q[2])).or(col("min").gt(lit(q[3]))))))
                    .withColumn("my_cnt",
                            when(col("min").geq(q[2]).and(col("max").leq(q[3])), col("cnt"))
                                    .otherwise(when(col("min").lt(q[2]), datediff(lit(q[2]), col("min"))
                                            .divide(datediff(col("max"), col("min")))
                                            .multiply(col("cnt")).cast(DataTypes.LongType))
                                    .otherwise(
                                            datediff(col("max"), lit(q[3]))
                                                    .divide(datediff(col("max"), col("min"))
                                                            .multiply(col("cnt"))).cast(DataTypes.LongType)))
                    )
                    .groupBy().sum("my_cnt").as(Encoders.LONG()).collectAsList().get(0);

            long val3 = rootIndex
                    .where(col("col").equalTo("l_commitdate").and(not(col("max").lt(lit(q[4])).or(col("min").gt(lit(q[5]))))))
                    .withColumn("my_cnt",
                            when(col("min").geq(q[4]).and(col("max").leq(q[5])), col("cnt"))
                                    .otherwise(when(col("min").lt(q[4]), datediff(lit(q[4]), col("min"))
                                            .divide(datediff(col("max"), col("min")))
                                                    .multiply(col("cnt")).cast(DataTypes.LongType))
                                            .otherwise(
                                                    datediff(col("max"), lit(q[5]))
                                                            .divide(datediff(col("max"), col("min"))
                                                                    .multiply(col("cnt"))).cast(DataTypes.LongType)))
                    )
                    .groupBy().sum("my_cnt").as(Encoders.LONG()).collectAsList().get(0);

            long recordsEstimation = (long) ((val1 * 1.0 / N) * (val2 * 1.0 / N ) * val3);

            long filesEstimation = (long) (F * (1 - Math.pow(factor, recordsEstimation)));


            results.add(Arrays.asList(q[0], String.valueOf(recordsEstimation), String.valueOf(filesEstimation)));
        }

        for (List<String> list : results){
            System.out.println(list.get(0) + " -- " + list.get(1)+ " -- " + list.get(2));
        }

    }

    @Test
    public void assignEstimationsTest(){

        SparkSession sparkSession = MyUtils.initTestSparkSession("estimationsForQueriesTest");

        Dataset rootIndex = sparkSession.read().json("src/test/resources/root-index/root.json").cache();

        //Dataset rootIndex = sparkSession.read().json("/Users/grishaw/dev/other/tpch/dbgen/index/v10/lineitem1/root-index/").cache();

        List<Clause>[] queries = new List[]{q1, q2, q3, q4, q5, q6, q7, q8, q9, q10, q11, q12, q13, q14};

        for (List<Clause> q : queries) {
            BqcppSolver.assignEstimations(q, rootIndex);
        }

        int i=1;
        for (List<Clause> q : queries) {
            System.out.println("q" + i++);
            q.forEach(System.out::println);
            System.out.println("------------------");
        }

    }



}
