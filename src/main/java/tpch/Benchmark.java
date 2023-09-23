package tpch;

import bqcpp.BqcppSolver;
import bqcpp.Clause;
import bqcpp.Plan;
import index.Index;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import static bqcpp.ClauseType.LESS_OR_EQUAL_THAN;
import static bqcpp.ClauseType.RANGE;
import static index.Index.initSparkSession;
import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.functions.col;

public class Benchmark {

    public static final List<Clause> q1 = Arrays.asList(
            new Clause(LESS_OR_EQUAL_THAN, "l_extendedprice", "969", null),
            new Clause(RANGE, "l_shipdate", "1994-01-06", "1994-01-10"),
            new Clause(RANGE, "l_commitdate", "1994-01-01", "1994-01-05")
    );

    public static final List<Clause> q2 = Arrays.asList(
            new Clause(LESS_OR_EQUAL_THAN, "l_extendedprice", "972", null),
            new Clause(RANGE, "l_shipdate", "1994-01-10", "1994-01-20"),
            new Clause(RANGE, "l_commitdate", "1994-01-01", "1994-01-09")
    );

    public static final List<Clause> q3 = Arrays.asList(
            new Clause(LESS_OR_EQUAL_THAN, "l_extendedprice", "980", null),
            new Clause(RANGE, "l_shipdate", "1994-01-16", "1994-01-26"),
            new Clause(RANGE, "l_commitdate", "1994-01-01", "1994-01-15")
    );

    public static final List<Clause> q4 = Arrays.asList(
            new Clause(LESS_OR_EQUAL_THAN, "l_extendedprice", "1001", null),
            new Clause(RANGE, "l_shipdate", "1994-01-20", "1994-01-30"),
            new Clause(RANGE, "l_commitdate", "1994-01-01", "1994-01-19")
    );

    public static final List<Clause> q5 = Arrays.asList(
            new Clause(LESS_OR_EQUAL_THAN, "l_extendedprice", "1030", null),
            new Clause(RANGE, "l_shipdate", "1994-01-24", "1994-02-01"),
            new Clause(RANGE, "l_commitdate", "1994-01-01", "1994-01-23")
    );

    public static final List<Clause> q6 = Arrays.asList(
            new Clause(LESS_OR_EQUAL_THAN, "l_extendedprice", "1050", null),
            new Clause(RANGE, "l_shipdate", "1994-01-26", "1994-02-03"),
            new Clause(RANGE, "l_commitdate", "1994-01-01", "1994-01-25")
    );

    public static final List<Clause> q7 = Arrays.asList(
            new Clause(LESS_OR_EQUAL_THAN, "l_extendedprice", "1079", null),
            new Clause(RANGE, "l_shipdate", "1994-01-31", "1994-02-06"),
            new Clause(RANGE, "l_commitdate", "1994-01-01", "1994-01-30")
    );

    public static final List<Clause> q8 = Arrays.asList(
            new Clause(LESS_OR_EQUAL_THAN, "l_extendedprice", "1092", null),
            new Clause(RANGE, "l_shipdate", "1994-01-31", "1994-02-07"),
            new Clause(RANGE, "l_commitdate", "1994-01-01", "1994-01-30")
    );

    public static final List<Clause> q9 = Arrays.asList(
            new Clause(LESS_OR_EQUAL_THAN, "l_extendedprice", "1098", null),
            new Clause(RANGE, "l_shipdate", "1994-01-31", "1994-02-09"),
            new Clause(RANGE, "l_commitdate", "1994-01-01", "1994-01-30")
    );

    public static final List<Clause> q10 = Arrays.asList(
            new Clause(LESS_OR_EQUAL_THAN, "l_extendedprice", "1105", null),
            new Clause(RANGE, "l_shipdate", "1994-02-01", "1994-02-12"),
            new Clause(RANGE, "l_commitdate", "1994-01-01", "1994-01-31")
    );

    public static final List<Clause> q11 = Arrays.asList(
            new Clause(LESS_OR_EQUAL_THAN, "l_extendedprice", "1115", null),
            new Clause(RANGE, "l_shipdate", "1994-02-01", "1994-02-16"),
            new Clause(RANGE, "l_commitdate", "1994-01-01", "1994-01-31")
    );

    public static final List<Clause> q12 = Arrays.asList(
            new Clause(LESS_OR_EQUAL_THAN, "l_extendedprice", "1122", null),
            new Clause(RANGE, "l_shipdate", "1994-02-01", "1994-02-20"),
            new Clause(RANGE, "l_commitdate", "1994-01-01", "1994-01-31")
    );

    public static final List<Clause> q13 = Arrays.asList(
            new Clause(LESS_OR_EQUAL_THAN, "l_extendedprice", "1137", null),
            new Clause(RANGE, "l_shipdate", "1994-02-01", "1994-03-01"),
            new Clause(RANGE, "l_commitdate", "1994-01-01", "1994-01-31")
    );

    public static final List<Clause> q14 = Arrays.asList(
            new Clause(LESS_OR_EQUAL_THAN, "l_extendedprice", "2000", null),
            new Clause(RANGE, "l_shipdate", "1994-02-01", "1994-03-30"),
            new Clause(RANGE, "l_commitdate", "1994-01-01", "1994-01-31")
    );

    public static void main(String[] args) {

        String tablePath = args[0];
        String indexPath = args[1];
        String tableFormat = args[2].trim().toLowerCase();
        BqcppSolver.F = Long.parseLong(args[3].trim());

        SparkSession sparkSession = initSparkSession("benchmark");

        runBenchmark(sparkSession, tablePath, indexPath, tableFormat);
    }


    public static void runBenchmark(SparkSession spark, String tablePath, String indexPath, String tableFormat){

        List<Clause>[] queries = new List[]{q1, q2, q3, q4, q5, q6, q7, q8, q9, q10, q11, q12, q13, q14};

        List<List<String>> result = new LinkedList<>();

        Dataset rootIndex = spark.read().json(indexPath + "/" + Index.rootIndexSuffix).cache();

        int j=1;
        for (List<Clause> q: queries) {

            int timeNoIndex=0, timeWithIndex=0;
            int numOfRetries = 3, numOfFiles = 0;

            long tightCoverageSize = getTightCoverageSize(TablesReader.readLineItemWithFormat(spark, tablePath, tableFormat), q);

            BqcppSolver.assignEstimations(q, rootIndex);

            Plan p = BqcppSolver.getBalancedPlanByGreedyAlgorithm(q);

            for (int i=0; i<numOfRetries; i++) {

                // test - no index
                long start = System.currentTimeMillis();

                Dataset lineItem = TablesReader.readLineItemWithFormat(spark, tablePath, tableFormat);
                double result1 = runBenchmarkQuery(lineItem, q);

                long end = System.currentTimeMillis();

                // test with index
                long start2 = System.currentTimeMillis();

                List<String> fileNames = p.getCoverage(spark, rootIndex);
                Dataset lineItemViaIndex = TablesReader.readLineItemWithFormat(spark, fileNames.toArray(new String[0]), tableFormat);
                double result2 = runBenchmarkQuery(lineItemViaIndex, q);

                long end2 = System.currentTimeMillis();

                if ( Math.abs(result1 - result2) > 0.0001){
                    throw new RuntimeException("results do not match, result1 = " + result1 + ", result2 = " + result2);
                }

                timeNoIndex += (end - start) / 1000;
                timeWithIndex += (end2 - start2) / 1000;

                numOfFiles += fileNames.size();
            }

            List<String> currentLog = Arrays.asList("query-" + j++,
                    String.valueOf(Math.floor(1.0 * timeNoIndex / numOfRetries)),
                    String.valueOf(Math.floor(1.0 * timeWithIndex / numOfRetries)),
                    String.valueOf(p.getCoverageSize()),
                    String.valueOf(numOfFiles / numOfRetries),
                    String.valueOf(tightCoverageSize),
                    String.valueOf(p.cost),
                    String.valueOf(p.clauses.size()));

            result.add(currentLog);

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
