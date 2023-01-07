package bqcpp;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.junit.jupiter.api.Test;
import testutils.MyUtils;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import static org.apache.spark.sql.functions.*;

public class BqcppSolverTest {


    static final String [] queryInput1 = {
            "query-1 (1% coverage)",
            "969",
            "1994-01-06", "1994-01-10",
            "1994-01-01", "1994-01-05"
    };

    static final String [] queryInput2 = {
            "query-2 (5% coverage)",
            "972",
            "1994-01-10", "1994-01-20",
            "1994-01-01", "1994-01-09"
    };

    static final String [] queryInput3 = {
            "query-3 (10% coverage)",
            "980",
            "1994-01-16", "1994-01-26",
            "1994-01-01", "1994-01-15"
    };

    static final String [] queryInput4 = {
            "query-4 (20% coverage)",
            "1001",
            "1994-01-20", "1994-01-30",
            "1994-01-01", "1994-01-19"
    };

    static final String [] queryInput5 = {
            "query-5 (30% coverage)",
            "1030",
            "1994-01-24", "1994-02-01",
            "1994-01-01", "1994-01-23"
    };

    static final String [] queryInput6 = {
            "query-6 (40% coverage)",
            "1050",
            "1994-01-26", "1994-02-03",
            "1994-01-01", "1994-01-25"
    };

    static final String [] queryInput7 = {
            "query-7 (50% coverage)",
            "1079",
            "1994-01-31", "1994-02-06",
            "1994-01-01", "1994-01-30"
    };

    static final String [] queryInput8 = {
            "query-8 (60% coverage)",
            "1092",
            "1994-01-31", "1994-02-07",
            "1994-01-01", "1994-01-30"
    };

    static final String [] queryInput9 = {
            "query-9 (70% coverage)",
            "1098",
            "1994-01-31", "1994-02-09",
            "1994-01-01", "1994-01-30"
    };

    static final String [] queryInput10 = {
            "query-10 (80% coverage)",
            "1105",
            "1994-02-01", "1994-02-12",
            "1994-01-01", "1994-01-31"
    };

    static final String [] queryInput11 = {
            "query-11 (90% coverage)",
            "1115",
            "1994-02-01", "1994-02-16",
            "1994-01-01", "1994-01-31"
    };

    static final String [] queryInput12 = {
            "query-12 (95% coverage)",
            "1122",
            "1994-02-01", "1994-02-20",
            "1994-01-01", "1994-01-31"
    };

    static final String [] queryInput13 = {
            "query-13 (99% coverage)",
            "1137",
            "1994-02-01", "1994-03-01",
            "1994-01-01", "1994-01-31"
    };

    static final String [] queryInput14 = {
            "query-14 (100% coverage)",
            "2000",
            "1994-02-01", "1994-03-30",
            "1994-01-01", "1994-01-31"
    };

    @Test
    public void estimationsForQueriesTest(){
        SparkSession sparkSession = MyUtils.initTestSparkSession("myTest");

        Dataset df = sparkSession.read().json("src/test/resources/root-index/root.json");


        String [][] queries = {queryInput1, queryInput2, queryInput3, queryInput4, queryInput5, queryInput6, queryInput7, queryInput8,
                queryInput9, queryInput10, queryInput11, queryInput12, queryInput13, queryInput14};


        final long N = 5999989709L;

        final long F = 10000;

        final double factor = 0.9999; // (F -1) / F

        List<List <String>> results = new LinkedList<>();
        for (String [] q : queries) {

            long val1 = df
                    .where(col("col").equalTo("l_extendedprice").and(col("min").leq(lit(Integer.valueOf(q[1])))))
                    .withColumn("my_cnt",
                            when(col("max").cast(DataTypes.LongType).leq(lit(Long.valueOf(q[1]))), col("cnt"))
                                    .otherwise((lit(Long.valueOf(q[1])).minus(col("min").cast(DataTypes.LongType)))
                                            .divide(col("max").cast(DataTypes.LongType).minus(col("min").cast(DataTypes.LongType)))
                                            .multiply(col("cnt"))).cast(DataTypes.LongType))
                    .groupBy().sum("my_cnt").as(Encoders.LONG()).collectAsList().get(0);

            long val2 = df
                    .where(col("col").equalTo("l_shipdate").and(not(col("max").lt(lit(q[2])).or(col("min").gt(lit(q[3]))))))
                    .withColumn("my_cnt",
                            when(col("min").geq(q[2]).and(col("max").leq(q[3])), col("cnt"))
                                    .otherwise(when(col("min").lt(q[2]), datediff(lit(q[2]), col("min"))
                                            .divide(datediff(col("max"), col("min"))
                                            .multiply(col("cnt"))).cast(DataTypes.LongType))
                                    .otherwise(
                                            datediff(col("max"), lit(q[3]))
                                                    .divide(datediff(col("max"), col("min"))
                                                            .multiply(col("cnt"))).cast(DataTypes.LongType)))
                    )
                    .groupBy().sum("my_cnt").as(Encoders.LONG()).collectAsList().get(0);

            long val3 = df
                    .where(col("col").equalTo("l_commitdate").and(not(col("max").lt(lit(q[4])).or(col("min").gt(lit(q[5]))))))
                    .withColumn("my_cnt",
                            when(col("min").geq(q[4]).and(col("max").leq(q[5])), col("cnt"))
                                    .otherwise(when(col("min").lt(q[4]), datediff(lit(q[4]), col("min"))
                                            .divide(datediff(col("max"), col("min"))
                                                    .multiply(col("cnt"))).cast(DataTypes.LongType))
                                            .otherwise(
                                                    datediff(col("max"), lit(q[5]))
                                                            .divide(datediff(col("max"), col("min"))
                                                                    .multiply(col("cnt"))).cast(DataTypes.LongType)))
                    )
                    .groupBy().sum("my_cnt").as(Encoders.LONG()).collectAsList().get(0);

            long recordsEstimation = (long) ((val1 * 1.0 / N) * (val2 * 1.0 / N ) * val3);

            long filesEstimation = (long) (F * (1 - Math.pow(factor, recordsEstimation)));


            results.add(Arrays.asList(q[0], String.valueOf(recordsEstimation), String.valueOf(filesEstimation)));
        }

        for (List<String> list : results){
            System.out.println(list.get(0) + " -- " + list.get(1)+ " -- " + list.get(2));
        }

    }

}
