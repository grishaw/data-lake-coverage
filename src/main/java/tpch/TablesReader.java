package tpch;

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

    public static Dataset readOrders(SparkSession spark, String path){
        return readOrders(spark, new String[]{path});
    }

    public static Dataset readOrders(SparkSession spark, String [] path){

        Dataset orders = spark
                .read()
                .option("delimiter", "|")
                .csv(path);

        orders = orders.withColumn("o_orderkey", col("_c0").cast(DataTypes.LongType));
        orders = orders.withColumn("o_custkey", col("_c1").cast(DataTypes.LongType));
        orders = orders.withColumn("o_orderstatus", col("_c2").cast(DataTypes.StringType));
        orders = orders.withColumn("o_totalprice", col("_c3").cast(DataTypes.DoubleType));


        orders = orders.withColumn("o_orderdate", col("_c4").cast(DataTypes.StringType));
        orders = orders.withColumn("o_orderpriority", col("_c5").cast(DataTypes.StringType));
        orders = orders.withColumn("o_clerk", col("_c6").cast(DataTypes.StringType));
        orders = orders.withColumn("o_shippriority", col("_c7").cast(DataTypes.LongType));

        orders = orders.withColumn("o_comment", col("_c8").cast(DataTypes.StringType));

        orders = orders.select("o_orderkey", "o_custkey", "o_orderstatus", "o_totalprice", "o_orderdate", "o_orderpriority",
                "o_clerk", "o_shippriority", "o_comment");

        orders.show();

        return orders;
    }

}
