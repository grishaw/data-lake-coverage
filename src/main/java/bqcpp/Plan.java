package bqcpp;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static bqcpp.BqcppSolver.*;
import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.functions.col;

public class Plan {

    public List<Clause> clauses = new ArrayList<>();

    public long cost;


    public long getTotalCost(){

        long result = clauses.get(0).result;
        for (int i=1; i<clauses.size(); i++){
            result = (long) (result * ((clauses.get(i).result * 1.0) / N));
        }

        long filesEstimation = (long) (F * (1 - Math.pow(FACTOR, result)));

        return filesEstimation  + cost;
    }

    public long getCoverageSize(){
        return getTotalCost() - cost;
    }

    // what would be the cost after adding given clause c to this clauses
    public long getExpectedTotalCost(Clause c){
        List<Clause> myClauses = new ArrayList<>(clauses);
        myClauses.add(c);

        long result = myClauses.get(0).result;
        for (int i=1; i<myClauses.size(); i++){
            result = (long) (result * ((myClauses.get(i).result * 1.0) / N));
        }

        long filesEstimation = (long) (F * (1 - Math.pow(FACTOR, result)));

        return filesEstimation  + cost;
    }

    // now assumes we benchmark query 6 only
    public List<String> getCoverage(SparkSession spark, Dataset rootIndex){

        Map <String, Clause> clausesMap = asMap();

        List<String> indexFileNamesExtendedPrice = rootIndex
                .where(col("col").equalTo("l_extendedprice").and(col("min").leq(lit(Integer.valueOf(clausesMap.get("l_extendedprice").columnValue1)))))
                .select("file").distinct().as(Encoders.STRING()).collectAsList();

        Dataset indexExtendedPrice = spark.read().parquet(indexFileNamesExtendedPrice.toArray(new java.lang.String[0]))
                .where(col("l_extendedprice").leq(lit(Integer.parseInt(clausesMap.get("l_extendedprice").columnValue1))));

        List<java.lang.String> indexFileNamesShipDate = rootIndex
                .where(col("col").equalTo("l_shipdate").and(not(col("max").lt(lit(clausesMap.get("l_shipdate").columnValue1)).or(col("min").gt(lit(clausesMap.get("l_shipdate").columnValue2))))))
                .select("file").distinct().as(Encoders.STRING()).collectAsList();
        Dataset indexShipDate = spark.read().parquet(indexFileNamesShipDate.toArray(new java.lang.String[0]))
                .where(col("l_shipdate").geq(clausesMap.get("l_shipdate").columnValue1).and(col("l_shipdate").leq(clausesMap.get("l_shipdate").columnValue2)));

        List<java.lang.String> indexFileNamesCommitDate = rootIndex
                .where(col("col").equalTo("l_commitdate").and(not(col("max").lt(lit(clausesMap.get("l_commitdate").columnValue1)).or(col("min").gt(lit(clausesMap.get("l_commitdate").columnValue2))))))
                .select("file").distinct().as(Encoders.STRING()).collectAsList();
        Dataset indexCommitDate = spark.read().parquet(indexFileNamesCommitDate.toArray(new java.lang.String[0]))
                .where(col("l_commitdate").geq(clausesMap.get("l_commitdate").columnValue1).and(col("l_commitdate").leq(clausesMap.get("l_commitdate").columnValue2)));

        Dataset joined = indexShipDate
                .join(indexExtendedPrice, indexShipDate.col("file").equalTo(indexExtendedPrice.col("file"))
                        .and(indexShipDate.col("id").equalTo(indexExtendedPrice.col("id"))))
                .join(indexCommitDate, indexShipDate.col("file").equalTo(indexCommitDate.col("file"))
                        .and(indexShipDate.col("id").equalTo(indexCommitDate.col("id"))))
                .select(indexShipDate.col("file"), indexCommitDate.col("id"));

        return (List<java.lang.String>) joined.select("file").distinct().as(Encoders.STRING()).collectAsList();
    }

    private Map<String, Clause> asMap(){
        Map<String, Clause> result = new HashMap<>();

        for (Clause c : clauses){
            result.put(c.columnName, c);
        }

        return result;
    }

    @Override
    public String toString() {
        return "Plan{" +
                "clauses=" + clauses.size() +
                ", cost=" + cost +
                ", coverage size =" + getCoverageSize() +
                ", total cost =" + getTotalCost() +
                '}';
    }
}
