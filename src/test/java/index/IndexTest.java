package index;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;
import tpch.TablesReader;

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

        Dataset root = sparkSession.read().json((indexPath + "/" + Index.rootIndexSuffix));

        root.show(100, false);

    }

}
