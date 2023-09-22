package tpch;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static testutils.MyUtils.initTestSparkSession;
import static org.apache.spark.sql.functions.*;

public class TablesReaderTest {

    @Test
    public void query1Validation(){
        SparkSession sparkSession = initTestSparkSession("query1Validation");

        Dataset lineItem = TablesReader.readLineItem(sparkSession, "src/test/resources/tables/lineitem.tbl");

        Dataset result = lineItem.filter(col("l_shipdate").leq("1998-09-02"))
                .groupBy("l_returnflag", "l_linestatus")
                .agg(sum("l_quantity"), sum("l_extendedprice"),
                        avg("l_quantity"), avg("l_extendedprice"), avg("l_discount"),
                        count("l_quantity").as("count_order"))
                .sort("l_returnflag", "l_linestatus");

        Assertions.assertEquals(1478493L, result.
                where("l_returnflag = 'A' and l_linestatus = 'F'")
                .select("count_order").as(Encoders.LONG()).collectAsList().get(0));
    }

    @Test
    public void query6Validation(){
        SparkSession sparkSession = initTestSparkSession("query6Validation");

        Dataset lineItem = TablesReader.readLineItem(sparkSession, "src/test/resources/tables/lineitem.tbl");

        double query6ExpectedResult = (double) lineItem
                .where("l_shipdate >= '1994-01-01' and l_shipdate < '1995-01-01' and l_discount >= 0.05 and l_discount <= 0.07 and l_quantity < 24")
                .agg(sum(col("l_extendedprice").multiply(col("l_discount")))).as(Encoders.DOUBLE()).collectAsList().get(0);

        Assertions.assertEquals(123141078.23, query6ExpectedResult, 0.1);
    }

    @Test
    public void query1ValidationParquet(){
        SparkSession sparkSession = initTestSparkSession("query1Validation");

        Dataset lineItem = TablesReader.readLineItemParquet(sparkSession, "src/test/resources/tables/parquet/lineitem/");

        Dataset result = lineItem.filter(col("l_shipdate").leq("1998-09-02"))
                .groupBy("l_returnflag", "l_linestatus")
                .agg(sum("l_quantity"), sum("l_extendedprice"),
                        avg("l_quantity"), avg("l_extendedprice"), avg("l_discount"),
                        count("l_quantity").as("count_order"))
                .sort("l_returnflag", "l_linestatus");

        Assertions.assertEquals(1478493L, result.
                where("l_returnflag = 'A' and l_linestatus = 'F'")
                .select("count_order").as(Encoders.LONG()).collectAsList().get(0));
    }

    @Test
    public void query6ValidationParquet(){
        SparkSession sparkSession = initTestSparkSession("query6ValidationParquet");

        Dataset lineItem = TablesReader.readLineItemParquet(sparkSession, "src/test/resources/tables/parquet/lineitem/");

        double query6ExpectedResult = (double) lineItem
                .where("l_shipdate >= '1994-01-01' and l_shipdate < '1995-01-01' and l_discount >= 0.05 and l_discount <= 0.07 and l_quantity < 24")
                .agg(sum(col("l_extendedprice").multiply(col("l_discount")))).as(Encoders.DOUBLE()).collectAsList().get(0);

        Assertions.assertEquals(123141078.23, query6ExpectedResult, 0.1);
    }

    @Test
    public void query1ValidationIceberg(){
        SparkSession sparkSession = initTestSparkSession("query1Validation");

        Dataset lineItem = TablesReader.readLineItemIceberg(sparkSession);

        Dataset result = lineItem.filter(col("l_shipdate").leq("1998-09-02"))
                .groupBy("l_returnflag", "l_linestatus")
                .agg(sum("l_quantity"), sum("l_extendedprice"),
                        avg("l_quantity"), avg("l_extendedprice"), avg("l_discount"),
                        count("l_quantity").as("count_order"))
                .sort("l_returnflag", "l_linestatus");

        Assertions.assertEquals(1478493L, result.
                where("l_returnflag = 'A' and l_linestatus = 'F'")
                .select("count_order").as(Encoders.LONG()).collectAsList().get(0));
    }

    @Test
    public void query6ValidationIceberg(){
        SparkSession sparkSession = initTestSparkSession("query6ValidationIceberg");

        Dataset lineItem = TablesReader.readLineItemIceberg(sparkSession);

        double query6ExpectedResult = (double) lineItem
                .where("l_shipdate >= '1994-01-01' and l_shipdate < '1995-01-01' and l_discount >= 0.05 and l_discount <= 0.07 and l_quantity < 24")
                .agg(sum(col("l_extendedprice").multiply(col("l_discount")))).as(Encoders.DOUBLE()).collectAsList().get(0);

        Assertions.assertEquals(123141078.23, query6ExpectedResult, 0.1);
    }
}
