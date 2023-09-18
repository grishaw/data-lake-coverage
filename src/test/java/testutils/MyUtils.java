package testutils;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import tpch.TablesReader;


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
                .set("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
                .set("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
                .set("spark.sql.catalog.local.type", "hadoop")
                .set("spark.sql.catalog.local.warehouse", "src/test/resources/tables/iceberg/");
                ;

        return SparkSession.builder()
                .config(sparkConf)
                .getOrCreate();
    }

    public static void main(String[] args) {
        SparkSession spark = initTestSparkSession("test1");

        // to convert csv file to parquet
        //TablesReader.writeLineItemAsParquet(spark, "src/test/resources/tables/lineitem.tbl", "src/test/resources/tables/parquet/");

        // to convert csv file to iceberg
        //TablesReader.writeLineItemAsIceberg(spark, "src/test/resources/tables/lineitem.tbl");
    }

}
