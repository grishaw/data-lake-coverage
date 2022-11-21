package bqcpp;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;
import testutils.MyUtils;
import java.util.List;

import static tpch.Benchmark.*;

public class BqcppSolverTest {

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
