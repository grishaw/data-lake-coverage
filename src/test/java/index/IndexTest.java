package index;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import tpch.TablesReader;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.sum;
import static scala.collection.JavaConverters.collectionAsScalaIterableConverter;
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

    @Test
    public void benchmarkTest(){

        SparkSession sparkSession = initTestSparkSession("benchmarkTest");

        String tablePath = "/Users/grishaw/dev/other/tpch/dbgen/lineitem10";
        String indexPath = "/Users/grishaw/dev/other/tpch/dbgen/index/v3/lineitem10/";

        String [] queryInput1 = {
                "query-1 (1% coverage)",
                "l_extendedprice <= 969",
                "l_shipdate >= '1994-01-06' and l_shipdate <= '1994-01-10'",
                "l_commitdate >= '1994-01-01' and l_commitdate <= '1994-01-05'"
        };

        String [] queryInput2 = {
                "query-2 (5% coverage)",
                "l_extendedprice <= 972",
                "l_shipdate >= '1994-01-10' and l_shipdate <= '1994-01-20'",
                "l_commitdate >= '1994-01-01' and l_commitdate <= '1994-01-09'"
        };

        String [] queryInput3 = {
                "query-3 (10% coverage)",
                "l_extendedprice <= 980",
                "l_shipdate >= '1994-01-16' and l_shipdate <= '1994-01-26'",
                "l_commitdate >= '1994-01-01' and l_commitdate <= '1994-01-15'"
        };

        String [] queryInput4 = {
                "query-4 (20% coverage)",
                "l_extendedprice <= 1001",
                "l_shipdate >= '1994-01-20' and l_shipdate <= '1994-01-30'",
                "l_commitdate >= '1994-01-01' and l_commitdate <= '1994-01-19'"
        };

        String [] queryInput5 = {
                "query-5 (30% coverage)",
                "l_extendedprice <= 1030",
                "l_shipdate >= '1994-01-24' and l_shipdate <= '1994-02-01'",
                "l_commitdate >= '1994-01-01' and l_commitdate <= '1994-01-23'"
        };

        String [] queryInput6 = {
                "query-6 (40% coverage)",
                "l_extendedprice <= 1050",
                "l_shipdate >= '1994-01-26' and l_shipdate <= '1994-02-03'",
                "l_commitdate >= '1994-01-01' and l_commitdate <= '1994-01-25'"
        };

        String [] queryInput7 = {
                "query-7 (50% coverage)",
                "l_extendedprice <= 1079",
                "l_shipdate >= '1994-01-31' and l_shipdate <= '1994-02-06'",
                "l_commitdate >= '1994-01-01' and l_commitdate <= '1994-01-30'"
        };

        String [] queryInput8 = {
                "query-8 (60% coverage)",
                "l_extendedprice <= 1092",
                "l_shipdate >= '1994-01-31' and l_shipdate <= '1994-02-07'",
                "l_commitdate >= '1994-01-01' and l_commitdate <= '1994-01-30'"
        };

        String [] queryInput9 = {
                "query-9 (70% coverage)",
                "l_extendedprice <= 1098",
                "l_shipdate >= '1994-01-31' and l_shipdate <= '1994-02-09'",
                "l_commitdate >= '1994-01-01' and l_commitdate <= '1994-01-30'"
        };

        String [] queryInput10 = {
                "query-10 (80% coverage)",
                "l_extendedprice <= 1105",
                "l_shipdate >= '1994-02-01' and l_shipdate <= '1994-02-12'",
                "l_commitdate >= '1994-01-01' and l_commitdate <= '1994-01-31'"
        };

        String [] queryInput11 = {
                "query-11 (90% coverage)",
                "l_extendedprice <= 1115",
                "l_shipdate >= '1994-02-01' and l_shipdate <= '1994-02-16'",
                "l_commitdate >= '1994-01-01' and l_commitdate <= '1994-01-31'"
        };

        String [] queryInput12 = {
                "query-12 (95% coverage)",
                "l_extendedprice <= 1122",
                "l_shipdate >= '1994-02-01' and l_shipdate <= '1994-02-20'",
                "l_commitdate >= '1994-01-01' and l_commitdate <= '1994-01-31'"
        };

        String [] queryInput13 = {
                "query-13 (99% coverage)",
                "l_extendedprice <= 1137",
                "l_shipdate >= '1994-02-01' and l_shipdate <= '1994-03-01'",
                "l_commitdate >= '1994-01-01' and l_commitdate <= '1994-01-31'"
        };

        String [] queryInput14 = {
                "query-14 (100% coverage)",
                "l_extendedprice <= 2000",
                "l_shipdate >= '1994-02-01' and l_shipdate <= '1994-03-30'",
                "l_commitdate >= '1994-01-01' and l_commitdate <= '1994-01-31'"
        };

        String [][] queries = {queryInput1, queryInput2, queryInput3, queryInput4, queryInput5, queryInput6,
                queryInput7, queryInput8, queryInput9, queryInput10, queryInput11, queryInput12, queryInput13, queryInput14};

        // warm up
        Dataset warmUp = TablesReader.readLineItem(sparkSession, tablePath);
        warmUp.count();

        // TODO check different subsets

        List<List<String>> result = new LinkedList<>();

        for (String[] queryInput : queries) {

            int num1=0, num2=0, num3=0;
            int numOfRetries = 3;
            int numOfFiles = 0;
            for (int i=0; i<numOfRetries; i++) {

                long start = System.currentTimeMillis();

                Dataset lineItem = TablesReader.readLineItem(sparkSession, tablePath);

                double result1 = runBenchmarkQuery(lineItem, queryInput);

                long end = System.currentTimeMillis();

                long start2a = System.currentTimeMillis();

                Dataset indexExtendedPrice = sparkSession.read().parquet(indexPath + "l_extendedprice").where(queryInput[1]);
                Dataset indexShipDate = sparkSession.read().parquet(indexPath + "l_shipdate").where(queryInput[2]);
                Dataset indexCommitDate = sparkSession.read().parquet(indexPath + "l_commitdate").where(queryInput[3]);

                Dataset joined = indexShipDate
                        .join(indexExtendedPrice, collectionAsScalaIterableConverter(Arrays.asList("file", "id")).asScala().toSeq())
                        .join(indexCommitDate, collectionAsScalaIterableConverter(Arrays.asList("file", "id")).asScala().toSeq())
                        .select(indexShipDate.col("file"), indexShipDate.col("id"));

                List<String> fileNames = (List<String>) joined.select("file").distinct().as(Encoders.STRING()).collectAsList();

                long start2b = System.currentTimeMillis();

                Dataset lineItemViaIndex = TablesReader.readLineItem(sparkSession, fileNames.toArray(new String[0]));
                double result2 = runBenchmarkQuery(lineItemViaIndex, queryInput);

                long end2 = System.currentTimeMillis();

                Assertions.assertEquals(result1, result2, 0.001);

                num1 += (end - start) / 1000;
                num2 += (end2 - start2a) / 1000;
                num3 += (end2 - start2b) / 1000;

                numOfFiles += fileNames.size();
            }

            result.add(Arrays.asList(queryInput[0],
                    String.valueOf(Math.floor(1.0 * num1 / numOfRetries)),
                    String.valueOf(Math.floor(1.0 * num2 / numOfRetries)),
                    String.valueOf(Math.floor(1.0 * num3 / numOfRetries)),
                    String.valueOf(numOfFiles / numOfRetries))
            );

        }

        for (List<String> list : result){
            System.out.println("--------------------------------------------");
            System.out.println(list.get(0));
            System.out.println("num of files : " + list.get(4));
            System.out.println("--------------------------------------------");
            System.out.println("no index took : " + list.get(1) + " seconds");
            System.out.println("with index took : " + list.get(2) + " seconds");
            System.out.println("with files only took : " + list.get(3) + " seconds");
            System.out.println("--------------------------------------------");
            System.out.println("--------------------------------------------");
            System.out.println("*********************************************");
        }

    }

    private static double runBenchmarkQuery(Dataset df, String[] queryInput){
        return df
                .where(queryInput[1])
                .where(queryInput[2])
                .where(queryInput[3])
                .groupBy()
                .agg(sum(col("l_extendedprice").multiply(col("l_discount"))))
                .as(Encoders.DOUBLE()).collectAsList().get(0);
    }
}
