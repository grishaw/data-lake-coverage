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
    public void query6Validation(){
        SparkSession sparkSession = initTestSparkSession("query6Validation");

        Dataset lineItem = TablesReader.readLineItem(sparkSession, "src/test/resources/tables/lineitem.tbl");

        double query6ExpectedResult = (double) lineItem
                .where("l_shipdate >= '1994-01-01' and l_shipdate < '1995-01-01' and l_discount >= 0.05 and l_discount <= 0.07 and l_quantity < 24")
                .agg(sum(col("l_extendedprice").multiply(col("l_discount")))).as(Encoders.DOUBLE()).collectAsList().get(0);

        Assertions.assertEquals(123141078.23, query6ExpectedResult, 0.1);
    }

    @Test
    public void query12Validation(){
        SparkSession sparkSession = initTestSparkSession("query12Validation");

        Dataset lineItem = TablesReader.readLineItem(sparkSession, "src/test/resources/tables/lineitem.tbl");

        Dataset order = TablesReader.readOrders(sparkSession, "src/test/resources/tables/orders.tbl");

        Dataset result = lineItem
                .filter("(l_shipmode == 'MAIL' or l_shipmode = 'SHIP') " +
                        "and l_commitdate < l_receiptdate and l_shipdate < l_commitdate and l_receiptdate >= '1994-01-01' and l_receiptdate < '1995-01-01'")
                .join(order, lineItem.col("l_orderkey").equalTo(order.col("o_orderkey")))
                .select("l_shipmode", "o_orderpriority")
                .withColumn("highPriority", when(col("o_orderpriority").equalTo("1-URGENT").or(col("o_orderpriority").equalTo("2-HIGH")), lit(1)).otherwise(lit(0)))
                .withColumn("lowPriority", when(col("o_orderpriority").notEqual("1-URGENT").and(col("o_orderpriority").notEqual("2-HIGH")), lit(1)).otherwise(lit(0)))
                .groupBy("l_shipmode")
                .agg(sum("highPriority").as("high_line_count"), sum("lowPriority").as("low_line_count"))
                .sort("l_shipmode");

        result.show();

        long highLineCount = (long) result.where("l_shipmode = 'MAIL'").select("high_line_count").as(Encoders.LONG()).collectAsList().get(0);
        Assertions.assertEquals(6202, highLineCount);

        long lowLineCount = (long) result.where("l_shipmode = 'MAIL'").select("low_line_count").as(Encoders.LONG()).collectAsList().get(0);
        Assertions.assertEquals(9324, lowLineCount);
    }
}
