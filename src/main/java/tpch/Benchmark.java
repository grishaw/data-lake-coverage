package tpch;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import static index.Index.initSparkSession;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.sum;

public class Benchmark {

    static final String [] queryInput1 = {
            "query-1 (1% coverage)",
            "l_extendedprice <= 969",
            "l_shipdate >= '1994-01-06' and l_shipdate <= '1994-01-10'",
            "l_commitdate >= '1994-01-01' and l_commitdate <= '1994-01-05'"
    };

    static final String [] queryInput2 = {
            "query-2 (5% coverage)",
            "l_extendedprice <= 972",
            "l_shipdate >= '1994-01-10' and l_shipdate <= '1994-01-20'",
            "l_commitdate >= '1994-01-01' and l_commitdate <= '1994-01-09'"
    };

    static final String [] queryInput3 = {
            "query-3 (10% coverage)",
            "l_extendedprice <= 980",
            "l_shipdate >= '1994-01-16' and l_shipdate <= '1994-01-26'",
            "l_commitdate >= '1994-01-01' and l_commitdate <= '1994-01-15'"
    };

    static final String [] queryInput4 = {
            "query-4 (20% coverage)",
            "l_extendedprice <= 1001",
            "l_shipdate >= '1994-01-20' and l_shipdate <= '1994-01-30'",
            "l_commitdate >= '1994-01-01' and l_commitdate <= '1994-01-19'"
    };

    static final String [] queryInput5 = {
            "query-5 (30% coverage)",
            "l_extendedprice <= 1030",
            "l_shipdate >= '1994-01-24' and l_shipdate <= '1994-02-01'",
            "l_commitdate >= '1994-01-01' and l_commitdate <= '1994-01-23'"
    };

    static final String [] queryInput6 = {
            "query-6 (40% coverage)",
            "l_extendedprice <= 1050",
            "l_shipdate >= '1994-01-26' and l_shipdate <= '1994-02-03'",
            "l_commitdate >= '1994-01-01' and l_commitdate <= '1994-01-25'"
    };

    static final String [] queryInput7 = {
            "query-7 (50% coverage)",
            "l_extendedprice <= 1079",
            "l_shipdate >= '1994-01-31' and l_shipdate <= '1994-02-06'",
            "l_commitdate >= '1994-01-01' and l_commitdate <= '1994-01-30'"
    };

    static final String [] queryInput8 = {
            "query-8 (60% coverage)",
            "l_extendedprice <= 1092",
            "l_shipdate >= '1994-01-31' and l_shipdate <= '1994-02-07'",
            "l_commitdate >= '1994-01-01' and l_commitdate <= '1994-01-30'"
    };

    static final String [] queryInput9 = {
            "query-9 (70% coverage)",
            "l_extendedprice <= 1098",
            "l_shipdate >= '1994-01-31' and l_shipdate <= '1994-02-09'",
            "l_commitdate >= '1994-01-01' and l_commitdate <= '1994-01-30'"
    };

    static final String [] queryInput10 = {
            "query-10 (80% coverage)",
            "l_extendedprice <= 1105",
            "l_shipdate >= '1994-02-01' and l_shipdate <= '1994-02-12'",
            "l_commitdate >= '1994-01-01' and l_commitdate <= '1994-01-31'"
    };

    static final String [] queryInput11 = {
            "query-11 (90% coverage)",
            "l_extendedprice <= 1115",
            "l_shipdate >= '1994-02-01' and l_shipdate <= '1994-02-16'",
            "l_commitdate >= '1994-01-01' and l_commitdate <= '1994-01-31'"
    };

    static final String [] queryInput12 = {
            "query-12 (95% coverage)",
            "l_extendedprice <= 1122",
            "l_shipdate >= '1994-02-01' and l_shipdate <= '1994-02-20'",
            "l_commitdate >= '1994-01-01' and l_commitdate <= '1994-01-31'"
    };

    static final String [] queryInput13 = {
            "query-13 (99% coverage)",
            "l_extendedprice <= 1137",
            "l_shipdate >= '1994-02-01' and l_shipdate <= '1994-03-01'",
            "l_commitdate >= '1994-01-01' and l_commitdate <= '1994-01-31'"
    };

    static final String [] queryInput14 = {
            "query-14 (100% coverage)",
            "l_extendedprice <= 2000",
            "l_shipdate >= '1994-02-01' and l_shipdate <= '1994-03-30'",
            "l_commitdate >= '1994-01-01' and l_commitdate <= '1994-01-31'"
    };

    public static void main(String[] args) {

        String tablePath = args[0];
        String indexPath = args[1];

        SparkSession sparkSession = initSparkSession("benchmark");

        runBenchmark(sparkSession, tablePath, indexPath);
    }

    private static void runBenchmark(SparkSession spark, String tablePath, String indexPath){

        String [][] queries = {queryInput1, queryInput2, queryInput3, queryInput4, queryInput5, queryInput6, queryInput7, queryInput8,
                queryInput9, queryInput10, queryInput11, queryInput12, queryInput13, queryInput14};

        // warm up
        Dataset warmUp = TablesReader.readLineItem(spark, tablePath);
        warmUp.count();

        // TODO check different subsets

        List<List<String>> result = new LinkedList<>();

        for (String[] queryInput : queries) {

            int num1=0, num2=0, num3=0;
            int numOfRetries = 3;
            int numOfFiles = 0;
            for (int i=0; i<numOfRetries; i++) {

                long start = System.currentTimeMillis();

                Dataset lineItem = TablesReader.readLineItem(spark, tablePath);

                double result1 = runBenchmarkQuery(lineItem, queryInput);

                long end = System.currentTimeMillis();

                long start2a = System.currentTimeMillis();

                Dataset indexExtendedPrice = spark.read().parquet(indexPath + "l_extendedprice").where(queryInput[1]);
                Dataset indexShipDate = spark.read().parquet(indexPath + "l_shipdate").where(queryInput[2]);
                Dataset indexCommitDate = spark.read().parquet(indexPath + "l_commitdate").where(queryInput[3]);

                Dataset joined = indexShipDate
                        .join(indexExtendedPrice, indexShipDate.col("file").equalTo(indexExtendedPrice.col("file"))
                                .and(indexShipDate.col("id").equalTo(indexExtendedPrice.col("id"))))
                        .join(indexCommitDate, indexShipDate.col("file").equalTo(indexCommitDate.col("file"))
                                .and(indexShipDate.col("id").equalTo(indexCommitDate.col("id"))))
                        .select(indexShipDate.col("file"), indexCommitDate.col("id"));

                List<String> fileNames = (List<String>) joined.select("file").distinct().as(Encoders.STRING()).collectAsList();

                long start2b = System.currentTimeMillis();

                Dataset lineItemViaIndex = TablesReader.readLineItem(spark, fileNames.toArray(new String[0]));
                double result2 = runBenchmarkQuery(lineItemViaIndex, queryInput);

                long end2 = System.currentTimeMillis();

                if ( Math.abs(result1 - result2) > 0.001){
                    throw new RuntimeException("results do not match, result1 = " + result1 + ", result2 = " + result2);
                }

                num1 += (end - start) / 1000;
                num2 += (end2 - start2a) / 1000;
                num3 += (end2 - start2b) / 1000;

                numOfFiles += fileNames.size();
            }

            result.add(Arrays.asList(queryInput[0],
                    String.valueOf(Math.floor(1.0 * num1 / numOfRetries)),
                    String.valueOf(Math.floor(1.0 * num2 / numOfRetries)),
                    String.valueOf(Math.floor(1.0 * num3 / numOfRetries)),
                    String.valueOf(numOfFiles / numOfRetries))
            );

        }

        for (List<String> list : result){
            System.out.println("--------------------------------------------");
            System.out.println(list.get(0));
            System.out.println("num of files : " + list.get(4));
            System.out.println("--------------------------------------------");
            System.out.println("no index took : " + list.get(1) + " seconds");
            System.out.println("with index took : " + list.get(2) + " seconds");
            System.out.println("with files only took : " + list.get(3) + " seconds");
            System.out.println("--------------------------------------------");
            System.out.println("--------------------------------------------");
            System.out.println("*********************************************");
        }
    }

    private static double runBenchmarkQuery(Dataset df, String[] queryInput){
        return df
                .where(queryInput[1])
                .where(queryInput[2])
                .where(queryInput[3])
                .groupBy()
                .agg(sum(col("l_extendedprice").multiply(col("l_discount"))))
                .as(Encoders.DOUBLE()).collectAsList().get(0);
    }


}
