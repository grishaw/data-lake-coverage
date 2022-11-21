package tpch;

import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;

import static tpch.Utils.initTestSparkSession;

public class UtilsTest {


    @Test
    public void initTestSparkSessionTest(){
        SparkSession sparkSession = initTestSparkSession("initTestSparkSessionTest");
        System.out.println(sparkSession.conf().getAll());
    }
}
