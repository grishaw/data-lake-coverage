# Optimizing cloud data lake queries with a balanced coverage plan
This is a proof-of-concept implementation of the ideas presented in the "Optimizing cloud data lake queries with a balanced coverage plan" paper.



## To perform the benchmark
 
### Create a TPC-H lineitem table

1. Download the `tpc-h_tools_v3.0.1.zip` file from the official TPC site - https://www.tpc.org/
2. Unzip it to get the `tpch` folder
3. Inside the `tpch/dbgen` folder create a makefile based on the template `tpch/dbgen/makefile.suite`
4. Run the following command to create a `lineitem` table comprised of 10k files with a scale factor of 1TB
   1. <code> for i in `seq 1 10000`; do echo $i; ./dbgen -s 1000 -S $i -C 10000 -T L -v; done; </code>
5. Copy lineitem files to the cloud storage bucket. For example, to copy to AWS S3, run
   1. `aws s3 sync . s3://your-benchmark-bucket/tpch/lineitem`

### Build the project
1. Clone this repo (`git clone https://github.com/grishaw/data-lake-coverage.git`)
2. Build the project without running unit tests - `mvn clean package -DskipTests`
3. To build the project and run the tests 
   1. `lineitem` table with scale factor 1GB should be placed in the `src/test/resources/tables` folder
   2. `mvn clean package`
4. After the build, `target/data-lake-coverage-1.0-SNAPSHOT.jar` file is created
5. Copy the jar to the cloud storage

### Create column indexes and the root index 
1. We create indexes by a Spark application running in the cloud (e.g., via AWS EMR). The following command should be executed on the Spark cluster:
   1. <code> spark-submit --deploy-mode cluster --class index.Index --deploy-mode cluster "path-to-jar" "path-to-lineitem-table" "path-to-index-folder" "comma separated column names to index" "max-records-per-index-file" "index-row-group-size-in-MB" "number-of-index-files-per-column" </code>
3. For example our command was:
   1. <code>spark-submit --deploy-mode cluster --class index.Index --deploy-mode cluster s3://data-lake-coverage/app/data-lake-coverage.jar s3://data-lake-coverage/tpch/lineitem s3://data-lake-coverage/index/lineitem l_shipdate,l_extendedprice,l_commitdate 20000000 128 1000</code>

### Run query evaluation
1. Should be run on a Spark cluster just like the previous step:
   1. <code>spark-submit --deploy-mode cluster --class tpch.Benchmark --deploy-mode cluster "path-to-jar" "path-to-lineitem-table" "path-to-index-folder" </code>
2. For example our command was:
   1. 1. <code>spark-submit --deploy-mode cluster --class tpch.Benchmark --deploy-mode cluster s3://data-lake-coverage/app/data-lake-coverage.jar s3://data-lake-coverage/tpch/lineitem s3://data-lake-coverage/index/lineitem/</code>

### Benchmark output
Output is written to the stdout of the driver. It should look as follows:
[TBD]

## Queries used in the benchmark

### We used the following template query which is based on TPC-H Query 6.

<code>
select sum(l_extendedprice*l_discount) <br>
&nbsp;from lineitem <br>
&nbsp;where l_discount >= 0.02 and l_discount <= 0.09 and l_quantity < 35 and <br>
&nbsp;l_extendedprice <= X1 and l_shipdate >= X2 and l_shipdate <= X3 and l_commitdate >= X4 and l_commitdate <= X5 <br>
</code>



### For each query, X values were replaced with the values from the following table

| query # | X1   | X 2        | X 3        | X 4        | X5         |
|---------|------|------------|------------|------------|------------|
| 1       | 969  | 1994-01-06 | 1994-01-10 | 1994-01-01 | 1994-01-05 |
| 2       | 972  | 1994-01-10 | 1994-01-20 | 1994-01-01 | 1994-01-09 |
| 3       | 980  | 1994-01-16 | 1994-01-26 | 1994-01-01 | 1994-01-15 |
| 4       | 1001 | 1994-01-20 | 1994-01-30 | 1994-01-01 | 1994-01-19 |
| 5       | 1030 | 1994-01-24 | 1994-02-01 | 1994-01-01 | 1994-01-23 |
| 6       | 1050 | 1994-01-26 | 1994-02-03 | 1994-01-01 | 1994-01-25 |
| 7       | 1079 | 1994-01-31 | 1994-02-06 | 1994-01-01 | 1994-01-30 |
| 8       | 1092 | 1994-01-31 | 1994-02-07 | 1994-01-01 | 1994-01-30 |
| 9       | 1098 | 1994-01-31 | 1994-02-09 | 1994-01-01 | 1994-01-30 |
| 10      | 1105 | 1994-02-01 | 1994-02-12 | 1994-01-01 | 1994-01-31 |
| 11      | 1115 | 1994-02-01 | 1994-02-16 | 1994-01-01 | 1994-01-31 |
| 12      | 1122 | 1994-02-01 | 1994-02-20 | 1994-01-01 | 1994-01-31 |
| 13      | 1137 | 1994-02-01 | 1994-03-01 | 1994-01-01 | 1994-01-31 |
| 14      | 2000 | 1994-02-01 | 1994-03-30 | 1994-01-01 | 1994-01-31 |



