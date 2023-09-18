package tpch;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

import static org.apache.spark.sql.functions.col;

public class TablesReader {

    public static Dataset readLineItem(SparkSession spark, String [] path){

        Dataset lineItem = spark
                .read()
                .option("delimiter", "|")
                .csv(path);

        lineItem = lineItem.withColumn("l_orderkey", col("_c0").cast(DataTypes.LongType));
        lineItem = lineItem.withColumn("l_partkey", col("_c1").cast(DataTypes.LongType));
        lineItem = lineItem.withColumn("l_suppkey", col("_c2").cast(DataTypes.LongType));
        lineItem = lineItem.withColumn("l_linenumber", col("_c3").cast(DataTypes.LongType));


        lineItem = lineItem.withColumn("l_quantity", col("_c4").cast(DataTypes.DoubleType));
        lineItem = lineItem.withColumn("l_extendedprice", col("_c5").cast(DataTypes.DoubleType));
        lineItem = lineItem.withColumn("l_discount", col("_c6").cast(DataTypes.DoubleType));
        lineItem = lineItem.withColumn("l_tax", col("_c7").cast(DataTypes.DoubleType));

        lineItem = lineItem.withColumnRenamed("_c8", "l_returnflag");
        lineItem = lineItem.withColumnRenamed("_c9", "l_linestatus");
        lineItem = lineItem.withColumnRenamed("_c10", "l_shipdate");
        lineItem = lineItem.withColumnRenamed("_c11", "l_commitdate");
        lineItem = lineItem.withColumnRenamed("_c12", "l_receiptdate");
        lineItem = lineItem.withColumnRenamed("_c13", "l_shipinstruct");
        lineItem = lineItem.withColumnRenamed("_c14", "l_shipmode");
        lineItem = lineItem.withColumnRenamed("_c15", "l_comment");

        lineItem = lineItem.select("l_orderkey", "l_partkey", "l_suppkey", "l_linenumber", "l_quantity" , "l_extendedprice",
                "l_discount", "l_tax", "l_returnflag", "l_linestatus", "l_shipdate", "l_commitdate" , "l_receiptdate",
                "l_shipinstruct", "l_shipmode" ,"l_comment");

        return lineItem;

    }

    public static Dataset readLineItem(SparkSession spark, String path){
        return readLineItem(spark, new String[]{path});
    }

    public static Dataset readLineItemParquet(SparkSession spark, String path){
        return readLineItemParquet(spark, new String[]{path});
    }

    public static Dataset readLineItemIceberg(SparkSession spark){
        return spark.read().format("iceberg").load("local.lineitem");
    }

    public static Dataset readLineItemParquet(SparkSession spark, String [] path) {
        return spark.read().parquet(path);
    }

    public static Dataset readLineItemWithFormat(SparkSession spark, String path, String tableFormat){
        Dataset lineItem;

        if (tableFormat.equals("csv")) {
            lineItem = TablesReader.readLineItem(spark, path);
        }else if (tableFormat.equals("parquet")){
            lineItem = TablesReader.readLineItemParquet(spark, path);
        }else if (tableFormat.equals("iceberg")){
            lineItem = TablesReader.readLineItemIceberg(spark);
        }else{
            throw new IllegalArgumentException("Invalid table format : " + tableFormat);
        }

        return lineItem;
    }

    public static Dataset readLineItemWithFormat(SparkSession spark, String[] path, String tableFormat){
        Dataset lineItem;
        if (tableFormat.equals("csv")) {
            lineItem = TablesReader.readLineItem(spark, path);
        }else if (tableFormat.equals("parquet")){
            lineItem = TablesReader.readLineItemParquet(spark, path);
        }else if (tableFormat.equals("iceberg")){
            lineItem = TablesReader.readLineItemParquet(spark, path);
        }else{
            throw new IllegalArgumentException("Invalid table format : " + tableFormat);
        }
        return lineItem;
    }


    // for tests only
    public static void writeLineItemAsParquet(SparkSession spark, String inputPath, String outputPath){
        Dataset lineItem = readLineItem(spark, inputPath);
        lineItem.write().parquet(outputPath);
    }

    public static void writeLineItemAsIceberg(SparkSession spark, String inputPath){
        Dataset lineItem = readLineItem(spark, inputPath);
        lineItem.writeTo("local.lineitem").create();
    }
}
