package bqcpp;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;
import testutils.MyUtils;
import java.util.Arrays;
import java.util.List;

import static bqcpp.ClauseType.LESS_OR_EQUAL_THAN;
import static bqcpp.ClauseType.RANGE;

public class BqcppSolverTest {

    List<Clause> q1 = Arrays.asList(
            new Clause(LESS_OR_EQUAL_THAN, "l_extendedprice", "969", null),
            new Clause(RANGE, "l_shipdate", "1994-01-06", "1994-01-10"),
            new Clause(RANGE, "l_commitdate", "1994-01-01", "1994-01-05")
    );

    List<Clause> q2 = Arrays.asList(
            new Clause(LESS_OR_EQUAL_THAN, "l_extendedprice", "972", null),
            new Clause(RANGE, "l_shipdate", "1994-01-10", "1994-01-20"),
            new Clause(RANGE, "l_commitdate", "1994-01-01", "1994-01-09")
    );

    List<Clause> q3 = Arrays.asList(
            new Clause(LESS_OR_EQUAL_THAN, "l_extendedprice", "980", null),
            new Clause(RANGE, "l_shipdate", "1994-01-16", "1994-01-26"),
            new Clause(RANGE, "l_commitdate", "1994-01-01", "1994-01-15")
    );

    List<Clause> q4 = Arrays.asList(
            new Clause(LESS_OR_EQUAL_THAN, "l_extendedprice", "1001", null),
            new Clause(RANGE, "l_shipdate", "1994-01-20", "1994-01-30"),
            new Clause(RANGE, "l_commitdate", "1994-01-01", "1994-01-19")
    );

    List<Clause> q5 = Arrays.asList(
            new Clause(LESS_OR_EQUAL_THAN, "l_extendedprice", "1030", null),
            new Clause(RANGE, "l_shipdate", "1994-01-24", "1994-02-01"),
            new Clause(RANGE, "l_commitdate", "1994-01-01", "1994-01-23")
    );

    List<Clause> q6 = Arrays.asList(
            new Clause(LESS_OR_EQUAL_THAN, "l_extendedprice", "1050", null),
            new Clause(RANGE, "l_shipdate", "1994-01-26", "1994-02-03"),
            new Clause(RANGE, "l_commitdate", "1994-01-01", "1994-01-25")
    );

    List<Clause> q7 = Arrays.asList(
            new Clause(LESS_OR_EQUAL_THAN, "l_extendedprice", "1079", null),
            new Clause(RANGE, "l_shipdate", "1994-01-31", "1994-02-06"),
            new Clause(RANGE, "l_commitdate", "1994-01-01", "1994-01-30")
    );

    List<Clause> q8 = Arrays.asList(
            new Clause(LESS_OR_EQUAL_THAN, "l_extendedprice", "1092", null),
            new Clause(RANGE, "l_shipdate", "1994-01-31", "1994-02-07"),
            new Clause(RANGE, "l_commitdate", "1994-01-01", "1994-01-30")
    );

    List<Clause> q9 = Arrays.asList(
            new Clause(LESS_OR_EQUAL_THAN, "l_extendedprice", "1098", null),
            new Clause(RANGE, "l_shipdate", "1994-01-31", "1994-02-09"),
            new Clause(RANGE, "l_commitdate", "1994-01-01", "1994-01-30")
    );

    List<Clause> q10 = Arrays.asList(
            new Clause(LESS_OR_EQUAL_THAN, "l_extendedprice", "1105", null),
            new Clause(RANGE, "l_shipdate", "1994-02-01", "1994-02-12"),
            new Clause(RANGE, "l_commitdate", "1994-01-01", "1994-01-31")
    );

    List<Clause> q11 = Arrays.asList(
            new Clause(LESS_OR_EQUAL_THAN, "l_extendedprice", "1115", null),
            new Clause(RANGE, "l_shipdate", "1994-02-01", "1994-02-16"),
            new Clause(RANGE, "l_commitdate", "1994-01-01", "1994-01-31")
    );

    List<Clause> q12 = Arrays.asList(
            new Clause(LESS_OR_EQUAL_THAN, "l_extendedprice", "1122", null),
            new Clause(RANGE, "l_shipdate", "1994-02-01", "1994-02-20"),
            new Clause(RANGE, "l_commitdate", "1994-01-01", "1994-01-31")
    );

    List<Clause> q13 = Arrays.asList(
            new Clause(LESS_OR_EQUAL_THAN, "l_extendedprice", "1137", null),
            new Clause(RANGE, "l_shipdate", "1994-02-01", "1994-03-01"),
            new Clause(RANGE, "l_commitdate", "1994-01-01", "1994-01-31")
    );

    List<Clause> q14 = Arrays.asList(
            new Clause(LESS_OR_EQUAL_THAN, "l_extendedprice", "2000", null),
            new Clause(RANGE, "l_shipdate", "1994-02-01", "1994-03-30"),
            new Clause(RANGE, "l_commitdate", "1994-01-01", "1994-01-31")
    );

    @Test
    public void assignEstimationsTest(){

        SparkSession sparkSession = MyUtils.initTestSparkSession("assignEstimationsTest");

        Dataset rootIndex = sparkSession.read().json("src/test/resources/root-index/root.json").cache();

        List<Clause>[] queries = new List[]{q1, q2, q3, q4, q5, q6, q7, q8, q9, q10, q11, q12, q13, q14};

        for (List<Clause> q : queries) {
            BqcppSolver.assignEstimations(q, rootIndex);
        }

        int i=1;
        for (List<Clause> q : queries) {
            System.out.println("q" + i++);
            q.forEach(System.out::println);
            System.out.println("------------------");
        }

    }

    @Test
    public void getBalancedPlanByGreedyAlgorithmTest(){

        SparkSession sparkSession = MyUtils.initTestSparkSession("getBalancedPlanByGreedyAlgorithmTest");

        Dataset rootIndex = sparkSession.read().json("src/test/resources/root-index/root.json").cache();

        List<Clause>[] queries = new List[]{q1, q2, q3, q4, q5, q6, q7, q8, q9, q10, q11, q12, q13, q14};

        for (List<Clause> q : queries) {
            BqcppSolver.assignEstimations(q, rootIndex);
        }

        for (List<Clause> q : queries) {
            Plan p = BqcppSolver.getBalancedPlanByGreedyAlgorithm(q);
            System.out.println(p);
            System.out.println("-----------------------------");
        }


    }

}
