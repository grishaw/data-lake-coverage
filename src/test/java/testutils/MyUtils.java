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
                .set("spark.sql.catalog.local.warehouse", "src/test/resources/tables/iceberg/")
                .set("spark.sql.catalog.local.table-override.write.parquet.compression-codec", "snappy")
                ;

        return SparkSession.builder()
                .config(sparkConf)
                .getOrCreate();
    }

}
