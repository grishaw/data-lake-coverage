package testutils;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import tpch.TablesReader;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import static scala.collection.JavaConverters.*;


import static org.apache.spark.sql.functions.input_file_name;
import static org.apache.spark.sql.functions.*;

public class MyUtils {

    public static SparkSession initTestSparkSession(String appName) {

        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkConf sparkConf = new SparkConf()
                .setAppName(appName)
                .setMaster("local[*, 2]")
                .set("spark.driver.host", "localhost")
                .set("spark.sql.shuffle.partitions", "5")
                .set("spark.default.parallelism", "5")
                .set("spark.sql.autoBroadcastJoinThreshold", "-1")
                ;

        return SparkSession.builder()
                .config(sparkConf)
                .getOrCreate();
    }

    @Test
    public void testCoverage(){

        SparkSession sparkSession = initTestSparkSession("testCoverage");

        Dataset lineItem = TablesReader.readLineItem(sparkSession, "/Users/grishaw/dev/other/tpch/dbgen/lineitem10");

        lineItem = lineItem.withColumn("file", input_file_name());

        lineItem.groupBy("l_shipdate", "l_discount", "l_quantity")
                .agg(count_distinct(col("file")).as("cnt")).where("cnt = 1")
                .show(false);
    }

    @Test
    public void benchmark(){

        SparkSession sparkSession = initTestSparkSession("benchmark");

        String tablePath = "/Users/grishaw/dev/other/tpch/dbgen/lineitem10";
        String indexPath = "/Users/grishaw/dev/other/tpch/dbgen/index/v3/lineitem10/";

        String [] queryInput1 = {
                "query-1 (1% coverage)",
                "l_extendedprice <= 969",
                "l_shipdate >= '1994-01-06' and l_shipdate <= '1994-01-10'",
                "l_commitdate >= '1994-01-01' and l_commitdate <= '1994-01-05'"
        };

        String [] queryInput2 = {
                "query-2 (5% coverage)",
                "l_extendedprice <= 972",
                "l_shipdate >= '1994-01-10' and l_shipdate <= '1994-01-20'",
                "l_commitdate >= '1994-01-01' and l_commitdate <= '1994-01-09'"
        };

        String [] queryInput3 = {
                "query-3 (10% coverage)",
                "l_extendedprice <= 980",
                "l_shipdate >= '1994-01-16' and l_shipdate <= '1994-01-26'",
                "l_commitdate >= '1994-01-01' and l_commitdate <= '1994-01-15'"
        };

        String [][] queries = {queryInput1, queryInput2, queryInput3};

        // warm up
        Dataset warmUp = TablesReader.readLineItem(sparkSession, tablePath);
        warmUp.count();

        // TODO perform each query 3 times and take average + check different subsets

        List<List<String>> result = new LinkedList<>();

        for (String[] queryInput : queries) {

            long start = System.currentTimeMillis();

            Dataset lineItem = TablesReader.readLineItem(sparkSession, tablePath);

            double result1 = runQuery(lineItem, queryInput);

            long end = System.currentTimeMillis();

            long start2a = System.currentTimeMillis();

            Dataset indexExtendedPrice = sparkSession.read().parquet(indexPath + "l_extendedprice").where(queryInput[1]);
            Dataset indexShipDate = sparkSession.read().parquet(indexPath + "l_shipdate").where(queryInput[2]);
            Dataset indexCommitDate = sparkSession.read().parquet(indexPath + "l_commitdate").where(queryInput[3]);

            Dataset joined = indexShipDate
                    .join(indexExtendedPrice, collectionAsScalaIterableConverter(Arrays.asList("file", "id")).asScala().toSeq())
                    .join(indexCommitDate, collectionAsScalaIterableConverter(Arrays.asList("file", "id")).asScala().toSeq())
                    .select(indexShipDate.col("file"), indexShipDate.col("id"));

            List<String> fileNames = (List<String>) joined.select("file").distinct().as(Encoders.STRING()).collectAsList();

            long start2b = System.currentTimeMillis();

            Dataset lineItemViaIndex = TablesReader.readLineItem(sparkSession, fileNames.toArray(new String[0]));
            double result2 = runQuery(lineItemViaIndex, queryInput);

            long end2 = System.currentTimeMillis();

            Assertions.assertEquals(result1, result2, 0.001);


            result.add(Arrays.asList(queryInput[0],
                    String.valueOf((end - start) / 1000),
                    String.valueOf((end2 - start2a) / 1000),
                    String.valueOf((end2 - start2b) / 1000))
            );

        }

        for (List<String> list : result){
            System.out.println("--------------------------------------------");
            System.out.println(list.get(0));
            System.out.println("--------------------------------------------");
            System.out.println("no index took : " + list.get(1) + " seconds");
            System.out.println("with index took : " + list.get(2) + " seconds");
            System.out.println("with files only took : " + list.get(3) + " seconds");
            System.out.println("--------------------------------------------");
            System.out.println("--------------------------------------------");
            System.out.println("*********************************************");
        }

    }

    private static double runQuery(Dataset df, String[] queryInput){
        return df
                .where(queryInput[1])
                .where(queryInput[2])
                .where(queryInput[3])
                .groupBy()
                .agg(sum(col("l_extendedprice").multiply(col("l_discount"))))
                .as(Encoders.DOUBLE()).collectAsList().get(0);
    }

}
