package tpch;

import index.Index;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import static index.Index.initSparkSession;
import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.functions.lit;

public class Benchmark {

    static final String [] queryInput1 = {
            "query-1 (1% coverage)",
            "969",
            "1994-01-06", "1994-01-10",
            "1994-01-01", "1994-01-05"
    };

    static final String [] queryInput2 = {
            "query-2 (5% coverage)",
            "972",
            "1994-01-10", "1994-01-20",
            "1994-01-01", "1994-01-09"
    };

    static final String [] queryInput3 = {
            "query-3 (10% coverage)",
            "980",
            "1994-01-16", "1994-01-26",
            "1994-01-01", "1994-01-15"
    };

    static final String [] queryInput4 = {
            "query-4 (20% coverage)",
            "1001",
            "1994-01-20", "1994-01-30",
            "1994-01-01", "1994-01-19"
    };

    static final String [] queryInput5 = {
            "query-5 (30% coverage)",
            "1030",
            "1994-01-24", "1994-02-01",
            "1994-01-01", "1994-01-23"
    };

    static final String [] queryInput6 = {
            "query-6 (40% coverage)",
            "1050",
            "1994-01-26", "1994-02-03",
            "1994-01-01", "1994-01-25"
    };

    static final String [] queryInput7 = {
            "query-7 (50% coverage)",
            "1079",
            "1994-01-31", "1994-02-06",
            "1994-01-01", "1994-01-30"
    };

    static final String [] queryInput8 = {
            "query-8 (60% coverage)",
            "1092",
            "1994-01-31", "1994-02-07",
            "1994-01-01", "1994-01-30"
    };

    static final String [] queryInput9 = {
            "query-9 (70% coverage)",
            "1098",
            "1994-01-31", "1994-02-09",
            "1994-01-01", "1994-01-30"
    };

    static final String [] queryInput10 = {
            "query-10 (80% coverage)",
            "1105",
            "1994-02-01", "1994-02-12",
            "1994-01-01", "1994-01-31"
    };

    static final String [] queryInput11 = {
            "query-11 (90% coverage)",
            "1115",
            "1994-02-01", "1994-02-16",
            "1994-01-01", "1994-01-31"
    };

    static final String [] queryInput12 = {
            "query-12 (95% coverage)",
            "1122",
            "1994-02-01", "1994-02-20",
            "1994-01-01", "1994-01-31"
    };

    static final String [] queryInput13 = {
            "query-13 (99% coverage)",
            "1137",
            "1994-02-01", "1994-03-01",
            "1994-01-01", "1994-01-31"
    };

    static final String [] queryInput14 = {
            "query-14 (100% coverage)",
            "2000",
            "1994-02-01", "1994-03-30",
            "1994-01-01", "1994-01-31"
    };

    public static void main(String[] args) {

        String tablePath = args[0];
        String indexPath = args[1];

        SparkSession sparkSession = initSparkSession("benchmark");

        runBenchmark(sparkSession, tablePath, indexPath);
    }

    private static void runBenchmark(SparkSession spark, String tablePath, String indexPath){

//        String [][] queries = {queryInput1, queryInput2, queryInput3, queryInput4, queryInput5, queryInput6, queryInput7, queryInput8,
//                queryInput9, queryInput10, queryInput11, queryInput12, queryInput13, queryInput14};

        String [][] queries = {queryInput1, queryInput2, queryInput13, queryInput14};

        // warm up
        Dataset warmUp = TablesReader.readLineItem(spark, tablePath);
        warmUp.count();

        // TODO check different subsets

        List<List<String>> result = new LinkedList<>();

        Dataset rootIndex = spark.read().json(indexPath + "/" + Index.rootIndexSuffix).cache();

        for (String[] queryInput : queries) {

            int num1=0, num2=0, num3=0;
            int numOfRetries = 3;
            int numOfFiles = 0, numOfIndexFiles1=0, numOfIndexFiles2=0, numOfIndexFiles3=0;;
            for (int i=0; i<numOfRetries; i++) {

                long start = System.currentTimeMillis();

                Dataset lineItem = TablesReader.readLineItem(spark, tablePath);

                double result1 = runBenchmarkQuery(lineItem, queryInput);

                long end = System.currentTimeMillis();

                long start2a = System.currentTimeMillis();

                List<String> indexFileNamesExtendedPrice = rootIndex
                        .where(col("col").equalTo("l_extendedprice").and(col("min").leq(lit(Integer.valueOf(queryInput[1])))))
                        .select("file").distinct().as(Encoders.STRING()).collectAsList();
                System.out.println("index files extendedPrice - " + indexFileNamesExtendedPrice);
                Dataset indexExtendedPrice = spark.read().parquet(indexFileNamesExtendedPrice.toArray(new String[0]))
                        .where(col("l_extendedprice").leq(lit(Integer.parseInt(queryInput[1]))));

                List<String> indexFileNamesShipDate = rootIndex
                        .where(col("col").equalTo("l_shipdate").and(not(col("max").lt(lit(queryInput[2])).or(col("min").gt(lit(queryInput[3]))))))
                        .select("file").distinct().as(Encoders.STRING()).collectAsList();
                System.out.println("index files l_shipdate - " + indexFileNamesShipDate);
                Dataset indexShipDate = spark.read().parquet(indexFileNamesShipDate.toArray(new String[0]))
                        .where(col("l_shipdate").geq(queryInput[2]).and(col("l_shipdate").leq(queryInput[3])));

                List<String> indexFileNamesCommitDate = rootIndex
                        .where(col("col").equalTo("l_commitdate").and(not(col("max").lt(lit(queryInput[4])).or(col("min").gt(lit(queryInput[5]))))))
                        .select("file").distinct().as(Encoders.STRING()).collectAsList();
                System.out.println("index files l_commitdate - " + indexFileNamesCommitDate);
                Dataset indexCommitDate = spark.read().parquet(indexFileNamesCommitDate.toArray(new String[0]))
                        .where(col("l_commitdate").geq(queryInput[4]).and(col("l_commitdate").leq(queryInput[5])));

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
                numOfIndexFiles1 += indexFileNamesExtendedPrice.size();
                numOfIndexFiles2 += indexFileNamesShipDate.size();
                numOfIndexFiles3 += indexFileNamesCommitDate.size();
            }

            result.add(Arrays.asList(queryInput[0],
                    String.valueOf(Math.floor(1.0 * num1 / numOfRetries)),
                    String.valueOf(Math.floor(1.0 * num2 / numOfRetries)),
                    String.valueOf(Math.floor(1.0 * num3 / numOfRetries)),
                    String.valueOf(numOfFiles / numOfRetries),
                    String.valueOf(numOfIndexFiles1 / numOfRetries),
                    String.valueOf(numOfIndexFiles2 / numOfRetries),
                    String.valueOf(numOfIndexFiles3 / numOfRetries))
            );

        }

        for (List<String> list : result){
            System.out.println("--------------------------------------------");
            System.out.println(list.get(0));
            System.out.println("num of files : " + list.get(4));
            System.out.println("num of l_extendedprice index files  : " + list.get(5));
            System.out.println("num of l_shipdate index files  : " + list.get(6));
            System.out.println("num of l_commitdate index files  : " + list.get(7));
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
                .where(col("l_extendedprice").leq(lit(Integer.parseInt(queryInput[1]))))
                .where(col("l_shipdate").geq(queryInput[2]).and(col("l_shipdate").leq(queryInput[3])))
                .where(col("l_commitdate").geq(queryInput[4]).and(col("l_commitdate").leq(queryInput[5])))
                .groupBy()
                .agg(sum(col("l_extendedprice").multiply(col("l_discount"))))
                .as(Encoders.DOUBLE()).collectAsList().get(0);
    }


}
