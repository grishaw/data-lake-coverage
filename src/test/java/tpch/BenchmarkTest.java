package tpch;

import index.Index;
import jdk.nashorn.internal.ir.annotations.Ignore;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import static org.apache.spark.sql.functions.*;
import static testutils.MyUtils.initTestSparkSession;

public class BenchmarkTest {

    @Test
    public void benchmarkTest(){

        SparkSession sparkSession = initTestSparkSession("benchmarkTest");

        String tablePath = "/Users/grishaw/dev/other/tpch/dbgen/lineitem10";
        String indexPath = "/Users/grishaw/dev/other/tpch/dbgen/index/v3/lineitem10/";

        String [][] queries = {Benchmark.queryInput1, Benchmark.queryInput7, Benchmark.queryInput14};

        // TODO check different subsets

        List<List<String>> result = new LinkedList<>();

        Dataset rootIndex = sparkSession.read().json(indexPath + "/" + Index.rootIndexSuffix).cache();

        for (String[] queryInput : queries) {

            int num1=0, num2=0, num3=0;
            int numOfRetries = 2;
            int numOfFiles = 0, numOfIndexFiles1=0, numOfIndexFiles2=0, numOfIndexFiles3=0;

            long tightCoverageSize = getTightCoverageSize(TablesReader.readLineItem(sparkSession, tablePath), queryInput);

            for (int i=0; i<numOfRetries; i++) {

                long start = System.currentTimeMillis();

                Dataset lineItem = TablesReader.readLineItem(sparkSession, tablePath);

                double result1 = runBenchmarkQuery(lineItem, queryInput);

                long end = System.currentTimeMillis();

                long start2a = System.currentTimeMillis();

                List<String> indexFileNamesExtendedPrice = rootIndex
                        .where(col("col").equalTo("l_extendedprice").and(col("min").leq(lit(Integer.valueOf(queryInput[1])))))
                        .select("file").distinct().as(Encoders.STRING()).collectAsList();
                System.out.println("index files extendedPrice - " + indexFileNamesExtendedPrice);
                Dataset indexExtendedPrice = sparkSession.read().parquet(indexFileNamesExtendedPrice.toArray(new String[0]))
                        .where(col("l_extendedprice").leq(lit(Integer.parseInt(queryInput[1]))));

                List<String> indexFileNamesShipDate = rootIndex
                        .where(col("col").equalTo("l_shipdate").and(not(col("max").lt(lit(queryInput[2])).or(col("min").gt(lit(queryInput[3]))))))
                        .select("file").distinct().as(Encoders.STRING()).collectAsList();
                System.out.println("index files l_shipdate - " + indexFileNamesShipDate);
                Dataset indexShipDate = sparkSession.read().parquet(indexFileNamesShipDate.toArray(new String[0]))
                        .where(col("l_shipdate").geq(queryInput[2]).and(col("l_shipdate").leq(queryInput[3])));

                List<String> indexFileNamesCommitDate = rootIndex
                        .where(col("col").equalTo("l_commitdate").and(not(col("max").lt(lit(queryInput[4])).or(col("min").gt(lit(queryInput[5]))))))
                        .select("file").distinct().as(Encoders.STRING()).collectAsList();
                System.out.println("index files l_commitdate - " + indexFileNamesCommitDate);
                Dataset indexCommitDate = sparkSession.read().parquet(indexFileNamesCommitDate.toArray(new String[0]))
                        .where(col("l_commitdate").geq(queryInput[4]).and(col("l_commitdate").leq(queryInput[5])));

                Dataset joined = indexShipDate
                        .join(indexExtendedPrice, indexShipDate.col("file").equalTo(indexExtendedPrice.col("file"))
                                .and(indexShipDate.col("id").equalTo(indexExtendedPrice.col("id"))))
                        .join(indexCommitDate, indexShipDate.col("file").equalTo(indexCommitDate.col("file"))
                                .and(indexShipDate.col("id").equalTo(indexCommitDate.col("id"))))
                        .select(indexShipDate.col("file"), indexCommitDate.col("id"));

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
                numOfIndexFiles1 += indexFileNamesExtendedPrice.size();
                numOfIndexFiles2 += indexFileNamesShipDate.size();
                numOfIndexFiles3 += indexFileNamesCommitDate.size();
            }

            result.add(Arrays.asList(queryInput[0],
                    String.valueOf(Math.floor(1.0 * num1 / numOfRetries)),
                    String.valueOf(Math.floor(1.0 * num2 / numOfRetries)),
                    String.valueOf(Math.floor(1.0 * num3 / numOfRetries)),
                    String.valueOf(numOfFiles / numOfRetries),
                    String.valueOf(numOfIndexFiles1 / numOfRetries),
                    String.valueOf(numOfIndexFiles2 / numOfRetries),
                    String.valueOf(numOfIndexFiles3 / numOfRetries),
                    String.valueOf(tightCoverageSize)
                    )
            );

        }

        // query 1 should have one file
        Assertions.assertEquals(1, Integer.parseInt(result.get(0).get(4)));

        for (List<String> list : result){
            System.out.println("--------------------------------------------");
            System.out.println(list.get(0));
            System.out.println("num of coverage files : " + list.get(4));
            System.out.println("num of tight coverage files : " + list.get(8));
            System.out.println("num of l_extendedprice index files  : " + list.get(5));
            System.out.println("num of l_shipdate index files  : " + list.get(6));
            System.out.println("num of l_commitdate index files  : " + list.get(7));
            System.out.println("--------------------------------------------");
            System.out.println("no index took : " + list.get(1) + " seconds");
            System.out.println("with index (via root) took : " + list.get(2) + " seconds");
            System.out.println("with files only took : " + list.get(3) + " seconds");
            System.out.println("--------------------------------------------");
            System.out.println("--------------------------------------------");
            System.out.println("*********************************************");
        }

    }

    private static long getTightCoverageSize(Dataset df, String[] queryInput){
        return df
                .where(getQuery6Condition(queryInput))
                .select(input_file_name()).distinct().count();
    }

    private static double runBenchmarkQuery(Dataset df, String[] queryInput){
        return df
                .where(getQuery6Condition(queryInput))
                .groupBy()
                .agg(sum(col("l_extendedprice").multiply(col("l_discount"))))
                .as(Encoders.DOUBLE()).collectAsList().get(0);
    }

    private static Column getQuery6Condition(String[] q){
        return col("l_extendedprice").leq(lit(Integer.parseInt(q[1])))
                .and(col("l_shipdate").geq(q[2])).and(col("l_shipdate").leq(q[3]))
                .and(col("l_commitdate").geq(q[4])).and(col("l_commitdate").leq(q[5]))
                .and(col("l_discount").geq(0.02)).and(col("l_discount").leq( 0.09))
                .and(col("l_quantity").lt(35));
    }

}
