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
   2. parquet version of `lineitem` (previous step) should be placed in `src/test/resources/tables/parquet/lineitem/` (MyUtils.main can be used)
   3. `mvn clean package`
4. After the build, `target/data-lake-coverage-1.0-SNAPSHOT.jar` file is created
5. Copy the jar to the cloud storage

### Create column indexes and the root index 
1. We create indexes by a Spark application running in the cloud (e.g., via AWS EMR). The following command should be executed on the Spark cluster:
   1. <code> spark-submit --deploy-mode cluster --class index.Index --deploy-mode cluster "path-to-jar" "path-to-lineitem-table" "path-to-index-folder" "comma separated column names to index" "max-records-per-index-file" "index-row-group-size-in-MB" "number-of-index-files-per-column" "table-format(csv or parquet)"</code>
3. For example our command was:
   1. <code>spark-submit --deploy-mode cluster --class index.Index --deploy-mode cluster s3://data-lake-coverage/app/data-lake-coverage.jar s3://data-lake-coverage/tpch/lineitem s3://data-lake-coverage/index/lineitem l_shipdate,l_extendedprice,l_commitdate 20000000 128 1000 csv</code>

### Run query evaluation
1. Should be run on a Spark cluster just like the previous step:
   1. <code>spark-submit --deploy-mode cluster --class tpch.Benchmark --deploy-mode cluster "path-to-jar" "path-to-lineitem-table" "path-to-index-folder" "table-format(csv or parquet)"</code>
2. For example our command was:
   1. 1. <code>spark-submit --deploy-mode cluster --class tpch.Benchmark --deploy-mode cluster s3://data-lake-coverage/app/data-lake-coverage.jar s3://data-lake-coverage/tpch/lineitem s3://data-lake-coverage/index/lineitem/ csv</code>

### Benchmark output
Output is written to the stdout of the driver. It should look as follows:
```
--------------------------------------------
query-1
--------------------------------------------
no index took : 329.0 seconds
with index took : 17.0 seconds
--------------------------------------------
num of estimated coverage files : 22
num of actual coverage files : 107
--------------------------------------------
num of tight coverage files : 73
num of index files  : 7
num of clauses  : 3
*********************************************
--------------------------------------------
query-2
--------------------------------------------
no index took : 328.0 seconds
with index took : 35.0 seconds
--------------------------------------------
num of estimated coverage files : 84
num of actual coverage files : 514
--------------------------------------------
num of tight coverage files : 374
num of index files  : 11
num of clauses  : 3
*********************************************
--------------------------------------------
query-3
--------------------------------------------
no index took : 328.0 seconds
with index took : 52.0 seconds
--------------------------------------------
num of estimated coverage files : 119
num of actual coverage files : 1029
--------------------------------------------
num of tight coverage files : 753
num of index files  : 13
num of clauses  : 3
*********************************************
--------------------------------------------
query-4
--------------------------------------------
no index took : 330.0 seconds
with index took : 85.0 seconds
--------------------------------------------
num of estimated coverage files : 260
num of actual coverage files : 2023
--------------------------------------------
num of tight coverage files : 1530
num of index files  : 16
num of clauses  : 3
*********************************************
--------------------------------------------
query-5
--------------------------------------------
no index took : 328.0 seconds
with index took : 120.0 seconds
--------------------------------------------
num of estimated coverage files : 206
num of actual coverage files : 3039
--------------------------------------------
num of tight coverage files : 2343
num of index files  : 15
num of clauses  : 3
*********************************************
--------------------------------------------
query-6
--------------------------------------------
no index took : 327.0 seconds
with index took : 157.0 seconds
--------------------------------------------
num of estimated coverage files : 281
num of actual coverage files : 4029
--------------------------------------------
num of tight coverage files : 3163
num of index files  : 17
num of clauses  : 3
*********************************************
--------------------------------------------
query-7
--------------------------------------------
no index took : 328.0 seconds
with index took : 186.0 seconds
--------------------------------------------
num of estimated coverage files : 449
num of actual coverage files : 5006
--------------------------------------------
num of tight coverage files : 3944
num of index files  : 19
num of clauses  : 3
*********************************************
--------------------------------------------
query-8
--------------------------------------------
no index took : 327.0 seconds
with index took : 219.0 seconds
--------------------------------------------
num of estimated coverage files : 420
num of actual coverage files : 6000
--------------------------------------------
num of tight coverage files : 4823
num of index files  : 19
num of clauses  : 3
*********************************************
--------------------------------------------
query-9
--------------------------------------------
no index took : 328.0 seconds
with index took : 253.0 seconds
--------------------------------------------
num of estimated coverage files : 658
num of actual coverage files : 7023
--------------------------------------------
num of tight coverage files : 5832
num of index files  : 20
num of clauses  : 3
*********************************************
--------------------------------------------
query-10
--------------------------------------------
no index took : 328.0 seconds
with index took : 288.0 seconds
--------------------------------------------
num of estimated coverage files : 930
num of actual coverage files : 8013
--------------------------------------------
num of tight coverage files : 6930
num of index files  : 22
num of clauses  : 3
*********************************************
--------------------------------------------
query-11
--------------------------------------------
no index took : 329.0 seconds
with index took : 327.0 seconds
--------------------------------------------
num of estimated coverage files : 1398
num of actual coverage files : 9032
--------------------------------------------
num of tight coverage files : 8184
num of index files  : 24
num of clauses  : 3
*********************************************
--------------------------------------------
query-12
--------------------------------------------
no index took : 329.0 seconds
with index took : 341.0 seconds
--------------------------------------------
num of estimated coverage files : 1689
num of actual coverage files : 9515
--------------------------------------------
num of tight coverage files : 8945
num of index files  : 25
num of clauses  : 3
*********************************************
--------------------------------------------
query-13
--------------------------------------------
no index took : 328.0 seconds
with index took : 356.0 seconds
--------------------------------------------
num of estimated coverage files : 2537
num of actual coverage files : 9900
--------------------------------------------
num of tight coverage files : 9683
num of index files  : 29
num of clauses  : 3
*********************************************
--------------------------------------------
query-14
--------------------------------------------
no index took : 328.0 seconds
with index took : 347.0 seconds
--------------------------------------------
num of estimated coverage files : 10000
num of actual coverage files : 10000
--------------------------------------------
num of tight coverage files : 10000
num of index files  : 13
num of clauses  : 1
*********************************************
```

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



