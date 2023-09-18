package index;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import tpch.TablesReader;

import static org.apache.spark.sql.functions.*;

public class Index {

    public final static String rootIndexSuffix = "/root-index";

    public static void main(String[] args) {

        String tablePath = args[0];
        String indexPath = args[1];

        String[] columns = args[2].split(",");

        int maxRecordsPerFile = Integer.parseInt(args[3]);
        int chunkSizeInMB = Integer.parseInt(args[4]);
        int numOfFiles = Integer.parseInt(args[5]);
        String tableFormat = args[6].trim().toLowerCase();

        SparkSession sparkSession = initSparkSession("createIndex");

        Dataset lineItem = TablesReader.readLineItemWithFormat(sparkSession, tablePath, tableFormat);

        Index.createLineItemIndex(lineItem, indexPath, columns, maxRecordsPerFile, chunkSizeInMB, numOfFiles);

    }

    public static void createLineItemIndex(Dataset df, String indexPath, String [] columns, int maxRecordsPerFile, int chunkSizeInMB, int numOfFiles){

        df.sparkSession().sparkContext().hadoopConfiguration().setInt("parquet.block.size", 1024 * 1024 * chunkSizeInMB);

        df = df
                .withColumn("file", input_file_name())
                .withColumn("id", concat(col("l_orderkey"), lit("_"), col("l_linenumber"))); //lineitem pk is (orderkey,linenumber)

        Dataset rootIndex = null;

        for (String col : columns) {

            // create index
            df
                    .select(col, "file", "id")
                    .orderBy(col)
                    .coalesce(numOfFiles)
                    .write()
                    .option("maxRecordsPerFile", maxRecordsPerFile)
                    .parquet(indexPath + "/" + col + "/");

            // create root index
            Dataset indexStored = df.sparkSession().read().parquet(indexPath + "/" + col + "/");

            Dataset currentRoot = indexStored
                    .groupBy(lit(col).as("col"), input_file_name().as("file"))
                    .agg(min(col).as("min"), max(col).as("max"), count(col).as("cnt"), countDistinct(col).as("cntd"));

            rootIndex = rootIndex == null ? currentRoot : rootIndex.unionByName(currentRoot);
        }

        rootIndex.coalesce(1).write().mode(SaveMode.Overwrite).json(indexPath + "/" + rootIndexSuffix);

    }

    public static SparkSession initSparkSession(String appName) {
        return SparkSession.builder().appName(appName).getOrCreate();
    }

}
