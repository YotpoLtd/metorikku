![Metorikku Logo](https://raw.githubusercontent.com/wiki/yotpoltd/metorikku/metorikku.png)

[![Build Status](https://travis-ci.org/YotpoLtd/metorikku.svg?branch=master)](https://travis-ci.org/YotpoLtd/metorikku)

Metorikku is a library that simplifies writing and executing ETLs on top of [Apache Spark](http://spark.apache.org/).
A user needs to write a simple YAML configuration file that includes SQL queries and run Metorikku on a spark cluster.
The platform also includes a way to write tests for metrics using MetorikkuTester.

### Getting started
To run Metorikku you must first define 2 files.

##### MQL file
An MQL (Metorikku Query Language) file defines the steps and queries of the ETL as well as where and what to output.

For example a simple configuration YAML (JSON is also supported) should be as follows:
```yaml
steps:
- dataFrameName: df1
  sql: 
    SELECT *
    FROM input_1
    WHERE id > 100
- dataFrameName: df2
  sql: 
    SELECT *
    FROM df1
    WHERE id < 1000
output:
- dataFrameName: df2
  outputType: Parquet
  outputOptions:
    saveMode: Overwrite
    path: df2.parquet
```
Take a look at the [examples file](https://github.com/YotpoLtd/metorikku/blob/master/examples) for further configuration examples.

##### Run configuration file
Metorikku uses a YAML file to describe the run configuration.
This file will include **input sources**, **output destinations** and the location of the **metric config** files.

So for example a simple YAML (JSON is also supported) should be as follows:
```yaml
metrics:
  - /full/path/to/your/MQL/file.yaml
inputs:
  input_1: parquet/input_1.parquet
  input_2: parquet/input_2.parquet
output:
    file:
        dir: /path/to/parquet/output
```
You can check out a full example file for all possible values in the [sample YAML configuration file](https://github.com/YotpoLtd/metorikku/blob/master/config/sample.yaml).

##### Supported input/output:

Currently Metorikku supports the following inputs:
**CSV, JSON, parquet, JDBC, Kafka**

And the following outputs:
**CSV, JSON, parquet, Redshift, Cassandra, Segment, JDBC, Kafka**<br />
***NOTE: If your are using Kafka as input note that the only supported outputs are currently Kafka and Parquet and currently you can use just one output for streaming metrics*** <br />
Redshift - s3_access_key and s3_secret are supported from spark-submit

### Running Metorikku
There are currently 3 options to run Metorikku.
##### Run on a spark cluster
*To run on a cluster Metorikku requires [Apache Spark](http://spark.apache.org/) v2.2+*
* Download the [last released JAR](https://github.com/YotpoLtd/metorikku/releases/latest)
* Run the following command:
     `spark-submit --class com.yotpo.metorikku.Metorikku metorikku.jar -c config.yaml`

#### Using JDBC
When using JDBC writer or input you must provide a path to the driver JAR.
For example to run with spark-submit with a mysql driver:
`spark-submit --driver-class-path mysql-connector-java-5.1.45.jar --jars mysql-connector-java-5.1.45.jar --class com.yotpo.metorikku.Metorikku metorikku.jar -c config.yaml`
If you want to run this with the standalone JAR:
`java -Dspark.master=local[*] -cp metorikku-standalone.jar:mysql-connector-java-5.1.45.jar -c config.yaml`

#### JDBC query
JDBC query output allows running a query for each record in the dataframe.

##### Mandatory parameters:
* **query** - defines the SQL query.
In the query you can address the column of the DataFrame by their location using the dollar sign ($) followed by the column index. For example:
```sql
INSERT INTO table_name (column1, column2, column3, ...) VALUES ($1, $2, $3, ...);
```
##### Optional Parameters:
* **maxBatchSize** - The maximum size of queries to execute against the DB in one commit.
* **minPartitions** - Minimum partitions in the DataFrame - may cause repartition.
* **maxPartitions** - Maximum partitions in the DataFrame - may cause coalesce.


#### Kafka output
Kafka output allows writing batch operations to kafka
We use spark-sql-kafka-0-10 as a provided jar - spark-submit command should look like so:

```spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.2.0 --class com.yotpo.metorikku.Metorikku metorikku.jar```

##### Mandatory parameters:
* **topic** - defines the topic in kafka which the data will be written to.
currently supported only one topic

* **valueColumn** - defines the values which will be written to the Kafka topic, 
Usually a json version of data, For example:
```sql
SELECT keyColumn, to_json(struct(*)) AS valueColumn FROM table
```
##### Optional Parameters:
* **keyColumn** - key that can be used to perform de-duplication when reading 

#### Kafka Input
Kafka input allows reading messages from topics
```yaml
inputs:
  testStream:
    kafka:
      servers:
        - 127.0.0.1:9092
      topic: test
      consumerGroup: testConsumerGroupID
```
Using Kafka input will convert your application into a streaming application build on top of Spark Structured Streaming. <br />
Please note the following while using streaming applications:

* Multiple streaming aggregations (i.e. a chain of aggregations on a streaming DF) are not yet supported on streaming Datasets.

* Limit and take first N rows are not supported on streaming Datasets.
* Distinct operations on streaming Datasets are not supported.

* Sorting operations are supported on streaming Datasets only after an aggregation and in Complete Output Mode.

* Make sure to add the relevant [Output Mode](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#output-modes) to your Metric as seen in the Examples

* Make sure to add the relevant [Triggers](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#triggers) to your Metric if needed as seen in the Examples

* For more information please go to [Spark Structured Streaming WIKI](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)

* In order to measure your consumer lag you can use the ```consumerGroup``` parameter to track your application offsets against your kafka input. <br />
This will commit the offsets to kafka, as a new dummy consumer group.



##### Run locally
*Metorikku is released with a JAR that includes a bundled spark.*
* Download the [last released Standalone JAR](https://github.com/YotpoLtd/metorikku/releases/latest)
* Run the following command:
`java -Dspark.master=local[*] -cp metorikku-standalone.jar com.yotpo.metorikku.Metorikku -c config.yaml`

##### Run as a library
*It's also possible to use Metorikku inside your own software*
*Metorikku library requires scala 2.11*
* Add the following dependency to your build.sbt:
`"com.yotpo" % "metorikku" % "0.0.1"`
* Start Metorikku by creating an instance of `com.yotpo.metorikku.config` and run `com.yotpo.metorikku.Metorikku.execute(config)`


#### Metorikku Tester
In order to test and fully automate the deployment of MQLs (Metorikku query language files) we added a method to run tests against MQLs.

A test is comprised of 2 files:
##### Test settings
This defines what to test and where to get the mocked data.
For example, a simple test YAML (JSON is also supported) will be:
```yaml
metric: "/path/to/metric"
mocks:
- name: table_1
  path: mocks/table_1.jsonl
tests:
  df2:
  - id: 200
    name: test
  - id: 300
    name: test2
```

And the corresponding `mocks/table_1.jsonl`:
```jsonl
{ "id": 200, "name": "test" }
{ "id": 300, "name": "test2" }
{ "id": 1, "name": "test3" }
```

##### Running Metorikku Tester
You can run Metorikku tester in any of the above methods (just like a normal Metorikku).
The main class changes from `com.yotpo.metorikku.Metorikku` to `com.yotpo.metorikku.MetorikkuTester`

## License  
See the [LICENSE](LICENSE.md) file for license rights and limitations (MIT).
