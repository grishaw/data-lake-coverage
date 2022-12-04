package index;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;
import tpch.TablesReader;

import java.util.List;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.sum;
import static testutils.MyUtils.initTestSparkSession;


public class IndexTest {

    @Test
    public void createLineItemIndexTest(){
        SparkSession sparkSession = initTestSparkSession("createLineItemIndexTest");

        Dataset lineItem = TablesReader.readLineItem(sparkSession, "src/test/resources/tables/lineitem.tbl");

        String indexPath = "target/index" + System.currentTimeMillis();
        Index.createLineItemIndex(lineItem, indexPath, new String[]{"l_shipdate", "l_discount", "l_quantity"}, 10_000_000, 100, 1);

        Dataset shipDateIndex = sparkSession.read().parquet(indexPath + "/"+"l_shipdate");
        shipDateIndex.show();

        Dataset discountIndex = sparkSession.read().parquet(indexPath + "/"+"l_discount");
        discountIndex.show();

        Dataset quantityIndex = sparkSession.read().parquet(indexPath + "/"+"l_quantity");
        quantityIndex.show();

    }

    @Test
    public void createIndexTest(){
        SparkSession sparkSession = initTestSparkSession("createIndexTest");

        Dataset lineItem = TablesReader.readLineItem(sparkSession, "src/test/resources/tables/lineitem.tbl");

        String indexPath = "target/index" + System.currentTimeMillis();
        Index.createLineItemIndex(lineItem, indexPath, new String[]{"l_shipdate"}, 100_000_000, 100, 1);

        Dataset result = sparkSession.read().parquet(indexPath + "/l_shipdate");

        result.show();

    }

    @Test
    public void indexTest(){
        SparkSession sparkSession = initTestSparkSession("indexTest");

        // warm up
        Dataset warmUp = TablesReader.readLineItem(sparkSession, "src/test/resources/tables/lineitem.tbl");
        warmUp.count();


        // test index
        long start1 = System.currentTimeMillis();

        Dataset indexShipDate = sparkSession.read().parquet("/Users/grishaw/dev/other/tpch/dbgen/index/v3/lineitem10/l_shipdate");
        Dataset indexDiscount = sparkSession.read().parquet("/Users/grishaw/dev/other/tpch/dbgen/index/v3/lineitem10/l_discount");
        Dataset indexQuantity = sparkSession.read().parquet("/Users/grishaw/dev/other/tpch/dbgen/index/v3/lineitem10/l_quantity");

        // 1 file out of 50 --> 10 sec vs 5
        indexShipDate = indexShipDate.where("l_shipdate = '1998-11-26'");
        indexDiscount = indexDiscount.where("l_discount = 0.05");
        indexQuantity = indexQuantity.where("l_quantity = 10");

        Dataset joined1 = indexShipDate.join(indexDiscount,
                        indexShipDate.col("file").equalTo(indexDiscount.col("file")).and(indexShipDate.col("id").equalTo(indexDiscount.col("id"))))
                .select(indexShipDate.col("file"), indexShipDate.col("id"));

        Dataset joined2 = joined1.join(indexQuantity,
                        joined1.col("file").equalTo(indexQuantity.col("file")).and(joined1.col("id").equalTo(indexQuantity.col("id"))))
                .select(joined1.col("file"), joined1.col("id"));


        List<String> fileName = (List<String>) joined2.select("file").distinct().as(Encoders.STRING()).collectAsList();
        System.out.println("number of files = " + fileName.size());
        Dataset lineItemViaIndex = TablesReader.readLineItem(sparkSession, fileName.toArray(new String[0]));

        double query6ExpectedResultIndex = runTestQuery(lineItemViaIndex);

        long end1 = System.currentTimeMillis();

        System.out.println("result with index : " + query6ExpectedResultIndex +", index result took : " + (end1 - start1) / 1000 + " seconds");


        // test no index
        long start = System.currentTimeMillis();

        Dataset lineItem = TablesReader.readLineItem(sparkSession, "/Users/grishaw/dev/other/tpch/dbgen/lineitem10");

        double query6ExpectedResult = runTestQuery(lineItem);

        long end = System.currentTimeMillis();

        System.out.println("result no index : " + query6ExpectedResult + ", took : " + (end - start) / 1000 + " seconds");

    }

    private double runTestQuery(Dataset df){
        return (double) df.where("l_shipdate = '1998-11-26' and l_discount = 0.05 and l_quantity = 10")
                .agg(sum(col("l_extendedprice").multiply(col("l_discount")))).as(Encoders.DOUBLE()).collectAsList().get(0);
    }
}
