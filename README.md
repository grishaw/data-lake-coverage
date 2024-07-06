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
   2. parquet version of `lineitem` (previous step) should be placed in `src/test/resources/tables/parquet/lineitem/`
   3. iceberg version of `lineitem` (previous steps) should be placed in `src/test/resources/tables/iceberg/lineitem/`
   4. `mvn clean package`
4. After the build, `target/data-lake-coverage-1.0-SNAPSHOT.jar` file is created
5. Copy the jar to the cloud storage

### Create column indexes and the root index 
1. We create indexes by a Spark application running in the cloud (e.g., via AWS EMR). The following command should be executed on the Spark cluster:
   1. <code> spark-submit --deploy-mode cluster --class index.Index --deploy-mode cluster "path-to-jar" "path-to-lineitem-table" "path-to-index-folder" "comma separated column names to index" "max-records-per-index-file" "index-row-group-size-in-MB" "number-of-index-files-per-column" "table-format(csv, parquet or iceberg)"</code>
3. For example our command was:
   1. <code>spark-submit --deploy-mode cluster --class index.Index --deploy-mode cluster s3://data-lake-coverage/app/data-lake-coverage.jar s3://data-lake-coverage/tpch/lineitem s3://data-lake-coverage/index/lineitem l_shipdate,l_extendedprice,l_commitdate 20000000 128 1000 csv</code>

### Run query evaluation
1. Should be run on a Spark cluster just like the previous step:
   1. <code>spark-submit --deploy-mode cluster --class tpch.Benchmark --deploy-mode cluster "path-to-jar" "path-to-lineitem-table" "path-to-index-folder" "table-format(csv, parquet or iceberg)" "number of files in the table" "query type (1 or 6)"</code>
2. For example one of our commands was:
   1. <code>spark-submit --deploy-mode cluster --class tpch.Benchmark --deploy-mode cluster s3://data-lake-coverage/app/data-lake-coverage.jar s3://data-lake-coverage/tpch/lineitem s3://data-lake-coverage/index/lineitem/ csv 10000 1</code>

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

### We used the following two template queries which are based on TPC-H Query 1 and Query 6.


Query 1

<code>
select 
 sum("l_quantity"), sum("l_extendedprice"),
 avg("l_quantity"), avg("l_extendedprice"), 
avg("l_discount"), count("l_quantity") <br>
&nbsp;from lineitem <br>
&nbsp;where l_discount >= 0.02 and l_discount <= 0.09 and l_quantity < 35 and <br>
&nbsp;l_extendedprice <= X1 and l_shipdate >= X2 and l_shipdate <= X3 and l_commitdate >= X4 and l_commitdate <= X5 <br>
group by l_returnflag, l_linestatus
order by l_returnflag, l_linestatus
</code>

Query 6

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


### Evaluation results 

#### Query 1 - 10k

| query # | no index csv | with index csv | no index iceberg | with index iceberg | no index parquet | with index parquet |
|---------|--------------|----------------|------------------|--------------------|------------------|--------------------|
| 1       | 408          | 19             | 92               | 22                 | 34               | 19                 |
| 2       | 402          | 38             | 81               | 22                 | 33               | 20                 |
| 3       | 405          | 58             | 82               | 27                 | 33               | 25                 |
| 4       | 399          | 98             | 80               | 32                 | 33               | 27                 |
| 5       | 398          | 140            | 81               | 33                 | 32               | 30                 |
| 6       | 397          | 180            | 83               | 39                 | 32               | 36                 |
| 7       | 396          | 218            | 81               | 42                 | 32               | 38                 |
| 8       | 401          | 259            | 81               | 47                 | 31               | 46                 |
| 9       | 396          | 300            | 81               | 53                 | 32               | 46                 |
| 10      | 395          | 338            | 81               | 57                 | 31               | 54                 |
| 11      | 395          | 381            | 80               | 60                 | 32               | 62                 |
| 12      | 392          | 402            | 80               | 64                 | 32               | 65                 |
| 13      | 390          | 415            | 80               | 68                 | 32               | 63                 |
| 14      | 391          | 408            | 82               | 56                 | 32               | 53                 |

#### Query 6 - 10k

| query # | no index csv | with index csv | no index iceberg | with index iceberg | no index parquet | with index parquet |
|---------|--------------|----------------|------------------|--------------------|------------------|--------------------|
| 1       | 329          | 17             | 79               | 22                 | 29               | 18                 |
| 2       | 328          | 35             | 67               | 22                 | 27               | 20                 |
| 3       | 328          | 52             | 66               | 24                 | 27               | 21                 |
| 4       | 330          | 85             | 68               | 28                 | 27               | 27                 |
| 5       | 328          | 120            | 66               | 32                 | 30               | 30                 |
| 6       | 327          | 157            | 66               | 39                 | 28               | 33                 |
| 7       | 328          | 186            | 66               | 50                 | 28               | 35                 |
| 8       | 327          | 219            | 66               | 45                 | 27               | 39                 |
| 9       | 328          | 253            | 67               | 48                 | 28               | 43                 |
| 10      | 328          | 288            | 64               | 58                 | 27               | 48                 |
| 11      | 329          | 327            | 65               | 59                 | 27               | 52                 |
| 12      | 329          | 341            | 65               | 60                 | 27               | 56                 |
| 13      | 328          | 356            | 65               | 66                 | 33               | 60                 |
| 14      | 328          | 347            | 65               | 51                 | 27               | 47                 |


#### Query 1 - 20k

| query # | no index csv | with index csv | no index iceberg | with index iceberg | no index parquet | with index parquet |
|---------|--------------|----------------|------------------|--------------------|------------------|--------------------|
| 1       | 390          | 17             | 125              | 18                 | 46               | 15                 |
| 2       | 388          | 31             | 124              | 22                 | 45               | 20                 |
| 3       | 390          | 43             | 122              | 27                 | 44               | 22                 |
| 4       | 388          | 64             | 121              | 28                 | 44               | 25                 |
| 5       | 389          | 88             | 120              | 31                 | 45               | 34                 |
| 6       | 387          | 114            | 124              | 36                 | 45               | 34                 |
| 7       | 393          | 143            | 124              | 43                 | 45               | 37                 |
| 8       | 395          | 179            | 123              | 48                 | 44               | 42                 |
| 9       | 399          | 213            | 124              | 50                 | 44               | 47                 |
| 10      | 400          | 253            | 123              | 58                 | 45               | 57                 |
| 11      | 399          | 312            | 122              | 65                 | 47               | 66                 |
| 12      | 400          | 344            | 125              | 74                 | 45               | 67                 |
| 13      | 399          | 400            | 124              | 85                 | 45               | 78                 |
| 14      | 400          | 424            | 123              | 73                 | 45               | 70                 |

#### Query 6 - 20k

| query # | no index csv | with index csv | no index iceberg | with index iceberg | no index parquet | with index parquet |
|---------|--------------|----------------|------------------|--------------------|------------------|--------------------|
| 1       | 385          | 18             | 112              | 17                 | 38               | 16                 |
| 2       | 382          | 30             | 94               | 22                 | 37               | 21                 |
| 3       | 384          | 42             | 93               | 23                 | 37               | 21                 |
| 4       | 382          | 65             | 92               | 26                 | 37               | 26                 |
| 5       | 384          | 90             | 95               | 31                 | 37               | 29                 |
| 6       | 382          | 118            | 94               | 35                 | 37               | 32                 |
| 7       | 385          | 140            | 91               | 37                 | 37               | 36                 |
| 8       | 383          | 173            | 93               | 45                 | 37               | 39                 |
| 9       | 384          | 225            | 92               | 47                 | 36               | 46                 |
| 10      | 382          | 244            | 91               | 51                 | 36               | 49                 |
| 11      | 379          | 292            | 90               | 62                 | 38               | 55                 |
| 12      | 376          | 329            | 92               | 63                 | 37               | 59                 |
| 13      | 377          | 377            | 92               | 71                 | 36               | 66                 |
| 14      | 376          | 402            | 92               | 64                 | 38               | 61                 |


#### Query 1 - 50k

| query # | no index csv | with index csv | no index iceberg | with index iceberg | no index parquet | with index parquet |
|---------|--------------|----------------|------------------|--------------------|------------------|--------------------|
| 1       | 454          | 17             | 271              | 23                 | 88               | 20                 |
| 2       | 454          | 26             | 267              | 23                 | 84               | 20                 |
| 3       | 453          | 32             | 264              | 25                 | 86               | 25                 |
| 4       | 456          | 44             | 257              | 31                 | 83               | 25                 |
| 5       | 455          | 57             | 259              | 31                 | 84               | 28                 |
| 6       | 460          | 74             | 258              | 36                 | 85               | 32                 |
| 7       | 457          | 91             | 256              | 40                 | 84               | 37                 |
| 8       | 457          | 112            | 252              | 46                 | 84               | 41                 |
| 9       | 459          | 131            | 263              | 51                 | 84               | 49                 |
| 10      | 458          | 161            | 259              | 60                 | 83               | 56                 |
| 11      | 458          | 209            | 253              | 65                 | 84               | 65                 |
| 12      | 456          | 249            | 256              | 76                 | 83               | 74                 |
| 13      | 459          | 326            | 255              | 95                 | 84               | 94                 |
| 14      | 453          | 494            | 247              | 125                | 84               | 126                |

#### Query 6 - 50k

| query # | no index csv | with index csv | no index iceberg | with index iceberg | no index parquet | with index parquet |
|---------|--------------|----------------|------------------|--------------------|------------------|--------------------|
| 1       | 444          | 17             | 200              | 24                 | 71               | 19                 |
| 2       | 441          | 27             | 194              | 24                 | 67               | 21                 |
| 3       | 437          | 31             | 188              | 24                 | 68               | 22                 |
| 4       | 444          | 42             | 188              | 26                 | 66               | 27                 |
| 5       | 439          | 58             | 182              | 35                 | 66               | 26                 |
| 6       | 442          | 69             | 202              | 34                 | 69               | 31                 |
| 7       | 438          | 86             | 196              | 36                 | 67               | 35                 |
| 8       | 440          | 110            | 194              | 41                 | 67               | 40                 |
| 9       | 442          | 128            | 188              | 48                 | 67               | 43                 |
| 10      | 439          | 157            | 195              | 52                 | 68               | 50                 |
| 11      | 442          | 202            | 193              | 61                 | 66               | 58                 |
| 12      | 441          | 240            | 189              | 70                 | 67               | 66                 |
| 13      | 440          | 320            | 187              | 84                 | 66               | 85                 |
| 14      | 440          | 479            | 187              | 107                | 68               | 107                |



#### Query 1 - 100k

| query # | no index csv | with index csv | no index iceberg | with index iceberg | no index parquet | with index parquet |
|---------|--------------|----------------|------------------|--------------------|------------------|--------------------|
| 1       | 590          | 20             | 535              | 18                 | 149              | 20                 |
| 2       | 580          | 24             | 549              | 24                 | 155              | 24                 |
| 3       | 586          | 33             | 531              | 28                 | 154              | 23                 |
| 4       | 607          | 37             | 532              | 30                 | 154              | 25                 |
| 5       | 611          | 47             | 514              | 32                 | 154              | 29                 |
| 6       | 616          | 59             | 497              | 35                 | 149              | 32                 |
| 7       | 616          | 72             | 508              | 42                 | 154              | 41                 |
| 8       | 612          | 86             | 484              | 44                 | 160              | 48                 |
| 9       | 602          | 102            | 540              | 50                 | 157              | 48                 |
| 10      | 603          | 124            | 532              | 57                 | 159              | 60                 |
| 11      | 614          | 165            | 535              | 72                 | 153              | 72                 |
| 12      | 600          | 196            | 529              | 84                 | 162              | 79                 |
| 13      | 616          | 277            | 510              | 107                | 163              | 124                |
| 14      | 608          | 698            | 506              | 213                | 159              | 223                |

#### Query 6 - 100k

| query # | no index csv | with index csv | no index iceberg | with index iceberg | no index parquet | with index parquet |
|---------|--------------|----------------|------------------|--------------------|------------------|--------------------|
| 1       | 617          | 20             | 522              | 19                 | 134              | 19                 |
| 2       | 610          | 25             | 466              | 24                 | 128              | 20                 |
| 3       | 630          | 31             | 485              | 25                 | 130              | 22                 |
| 4       | 615          | 38             | 447              | 28                 | 132              | 26                 |
| 5       | 616          | 47             | 468              | 31                 | 125              | 29                 |
| 6       | 623          | 61             | 472              | 36                 | 131              | 32                 |
| 7       | 636          | 75             | 467              | 39                 | 135              | 37                 |
| 8       | 624          | 88             | 433              | 44                 | 132              | 40                 |
| 9       | 622          | 104            | 430              | 47                 | 130              | 44                 |
| 10      | 622          | 126            | 428              | 55                 | 129              | 50                 |
| 11      | 624          | 168            | 414              | 64                 | 126              | 63                 |
| 12      | 617          | 201            | 413              | 75                 | 133              | 70                 |
| 13      | 609          | 281            | 412              | 98                 | 131              | 98                 |
| 14      | 612          | 690            | 389              | 194                | 127              | 197                |

