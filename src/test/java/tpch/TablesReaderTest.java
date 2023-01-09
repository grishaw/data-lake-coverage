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
}
