package tpch;

import bqcpp.BqcppSolver;
import bqcpp.Clause;
import bqcpp.Plan;
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
import static tpch.BenchmarkNew.*;

public class BenchmarkTest {

    @Ignore
    public void benchmarkTest(){

        // for local test set BqcppSolver.N = 6_001_215

        SparkSession sparkSession = initTestSparkSession("benchmarkTest");

        String tablePath = "/Users/grishaw/dev/other/tpch/dbgen/lineitem1/";
        String indexPath = "/Users/grishaw/dev/other/tpch/dbgen/index/v10/lineitem1/";

        List<Clause>[] queries = new List[]{q6, q12, q14};

        List<List<String>> result = new LinkedList<>();

        Dataset rootIndex = sparkSession.read().json(indexPath + "/" + Index.rootIndexSuffix).cache();

        int j=1;
        for (List<Clause> q: queries) {

            int timeNoIndex=0, timeWithIndex=0;
            int numOfRetries = 2, numOfFiles = 0;

            long tightCoverageSize = getTightCoverageSize(TablesReader.readLineItem(sparkSession, tablePath), q);

            BqcppSolver.assignEstimations(q, rootIndex);

            Plan p = BqcppSolver.getBalancedPlanByGreedyAlgorithm(q);

            for (int i=0; i<numOfRetries; i++) {

                // test - no index
                long start = System.currentTimeMillis();

                Dataset lineItem = TablesReader.readLineItem(sparkSession, tablePath);
                double result1 = runBenchmarkQuery(lineItem, q);

                long end = System.currentTimeMillis();

                // test with index
                long start2 = System.currentTimeMillis();

                List<String> fileNames = p.getCoverage(sparkSession, rootIndex);
                Dataset lineItemViaIndex = TablesReader.readLineItem(sparkSession, fileNames.toArray(new String[0]));
                double result2 = runBenchmarkQuery(lineItemViaIndex, q);

                long end2 = System.currentTimeMillis();

                Assertions.assertEquals(result1, result2, 0.001);

                timeNoIndex += (end - start) / 1000;
                timeWithIndex += (end2 - start2) / 1000;

                numOfFiles += fileNames.size();
            }

            result.add(Arrays.asList("query-" + j++,
                            String.valueOf(Math.floor(1.0 * timeNoIndex / numOfRetries)),
                            String.valueOf(Math.floor(1.0 * timeWithIndex / numOfRetries)),
                            String.valueOf(p.getCoverageSize()),
                            String.valueOf(numOfFiles / numOfRetries),
                            String.valueOf(tightCoverageSize),
                            String.valueOf(p.cost),
                            String.valueOf(p.clauses.size())
                    )
            );

        }

        for (List<String> list : result){
            System.out.println("--------------------------------------------");
            System.out.println(list.get(0));
            System.out.println("--------------------------------------------");
            System.out.println("no index took : " + list.get(1) + " seconds");
            System.out.println("with index took : " + list.get(2) + " seconds");
            System.out.println("--------------------------------------------");
            System.out.println("num of estimated coverage files : " + list.get(3));
            System.out.println("num of actual coverage files : " + list.get(4));
            System.out.println("--------------------------------------------");
            System.out.println("num of tight coverage files : " + list.get(5));
            System.out.println("num of index files  : " + list.get(6));
            System.out.println("num of clauses  : " + list.get(7));
            System.out.println("*********************************************");
        }

    }

    private static long getTightCoverageSize(Dataset df, List<Clause> q){
        return df
                .where(getQuery6Condition(q))
                .select(input_file_name()).distinct().count();
    }

    private static double runBenchmarkQuery(Dataset df, List<Clause> q){
        return df
                .where(getQuery6Condition(q))
                .groupBy()
                .agg(sum(col("l_extendedprice").multiply(col("l_discount"))))
                .as(Encoders.DOUBLE()).collectAsList().get(0);
    }

    private static Column getQuery6Condition(List<Clause> q){
        return col("l_extendedprice").leq(lit(Integer.parseInt(q.get(0).columnValue1)))
                .and(col("l_shipdate").geq(q.get(1).columnValue1)).and(col("l_shipdate").leq(q.get(1).columnValue2))
                .and(col("l_commitdate").geq(q.get(2).columnValue1)).and(col("l_commitdate").leq(q.get(2).columnValue2))
                .and(col("l_discount").geq(0.02)).and(col("l_discount").leq( 0.09))
                .and(col("l_quantity").lt(35));
    }

}
