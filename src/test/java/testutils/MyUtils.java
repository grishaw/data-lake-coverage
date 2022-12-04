package testutils;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;
import tpch.TablesReader;

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

}
