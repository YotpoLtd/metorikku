![Metorikku Logo](https://raw.githubusercontent.com/wiki/yotpoltd/metorikku/metorikku.png)

[![Build Status](https://travis-ci.org/YotpoLtd/metorikku.svg?branch=master)](https://travis-ci.org/YotpoLtd/metorikku)

[![Gitter](https://badges.gitter.im/metorikku/Lobby.svg)](https://gitter.im/metorikku/Lobby?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge)

Metorikku is a library that simplifies writing and executing ETLs on top of [Apache Spark](http://spark.apache.org/).

It is based on simple YAML configuration files and runs on any Spark cluster.

The platform also includes a simple way to write unit and E2E tests.

### Getting started
To run Metorikku you must first define 2 files.

#### Metric file
A metric file defines the steps and queries of the ETL as well as where and what to output.

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
You can check out a full example file for all possible values in the [sample YAML configuration file](config/metric_config_sample.yaml).

Make sure to also check out the full [Spark SQL Language manual](https://docs.databricks.com/spark/latest/spark-sql/index.html#sql-language-manual) for the possible queries.

#### Job file
This file will include **input sources**, **output destinations** and the location of the **metric config** files.

So for example a simple YAML (JSON is also supported) should be as follows:
```yaml
metrics:
  - /full/path/to/your/metric/file.yaml
inputs:
  input_1: parquet/input_1.parquet
  input_2: parquet/input_2.parquet
output:
    file:
        dir: /path/to/parquet/output
```
You can check out a full example file for all possible values in the [sample YAML configuration file](config/job_config_sample.yaml).

Also make sure to check out all our [examples](examples).

#### Supported input/output:

Currently Metorikku supports the following inputs:
**CSV, JSON, parquet, JDBC, Kafka, Cassandra, Elasticsearch**

And the following outputs:
**CSV, JSON, parquet, Redshift, Cassandra, Segment, JDBC, Kafka, Elasticsearch**<br />

### Running Metorikku
There are currently 3 options to run Metorikku.
#### Run on a spark cluster
*To run on a cluster Metorikku requires [Apache Spark](http://spark.apache.org/) v2.2+*
* Download the [last released JAR](https://github.com/YotpoLtd/metorikku/releases/latest)
* Run the following command:
     `spark-submit --class com.yotpo.metorikku.Metorikku metorikku.jar -c config.yaml`

#### Run locally
*Metorikku is released with a JAR that includes a bundled spark.*
* Download the [last released Standalone JAR](https://github.com/YotpoLtd/metorikku/releases/latest)
* Metorikku is required to be running with `Java 1.8`
* Run the following command:
`java -D"spark.master=local[*]" -cp metorikku-standalone.jar com.yotpo.metorikku.Metorikku -c config.yaml`
* Also job in a JSON format is supported, run following command:
`java -D"spark.master=local[*]" -cp metorikku-standalone.jar com.yotpo.metorikku.Metorikku --job "{*}"`
 
*Run locally in intellij:*

Go to Run->Edit Configuration->add application configuration

* Main Class: 
`com.yotpo.metorikku.Metorikku`
* Vm options: 
`-Dspark.master=local[*] -Dspark.executor.cores=1 -Dspark.driver.bindAddress=127.0.0.1 -Dspark.serializer=org.apache.spark.serializer.KryoSerializer`
* program arguments:
`-c examples/movies.yaml`
* JRE: `1.8`

*Run tester in intellij:*
* Main class: `com.yotpo.metorikku.MetorikkuTester`
* Program arguments: `--test-settings /{path to }/test_settings.yaml`


#### Run as a library
*It's also possible to use Metorikku inside your own software*

*Metorikku library requires scala 2.11*

To use it add the following dependency to your build.sbt:
`"com.yotpo" % "metorikku" % "LATEST VERSION"`

### Metorikku Tester
In order to test and fully automate the deployment of metrics we added a method to run tests against a metric.

A test is comprised of the following:
#### Test settings
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
keys:
  df2: 
  - id
  - name
```

And the corresponding `mocks/table_1.jsonl`:
```jsonl
{ "id": 200, "name": "test" }
{ "id": 300, "name": "test2" }
{ "id": 1, "name": "test3" }
```

The Keys section allows the user to define the unique columns of every DataFrame's expected results - 
every expected row result should have a unique combination for the values of the key columns. 
This part is optional and can be used to define only part of the expected DataFrames - 
for the DataFrames that don't have a key definition, all of the columns defined in the first row result 
will be taken by default as the unique keys. 
Defining a shorter list of key columns will result in better performances and a more detailed error message in case of test failure.

The structure of the defined expected dataFrame's result must be identical for all rows, and the keys must be valid 
(defined as columns of the expected results of the same DataFrame)


#### Running Metorikku Tester
You can run Metorikku tester in any of the above methods (just like a normal Metorikku).

The main class changes from `com.yotpo.metorikku.Metorikku` to `com.yotpo.metorikku.MetorikkuTester`

#### Testing streaming metrics
In Spark some behaviors are different when writing queries for streaming sources (for example kafka).

In order to make sure the test behaves the same as the real life queries, you can configure a mock to behave like a streaming input by writing the following:
```yaml
metric: "/path/to/metric"
mocks:
- name: table_1
  path: mocks/table_1.jsonl
  # default is false
  streaming: true
# default is append output mode
outputMode: update
tests:
  df2:
  - id: 200
    name: test
  - id: 300
    name: test2
```

### Notes

#### Variable interpolation
All configuration files support variable interpolation from environment variables and system properties using the following format:
`${variable_name}`

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

### Streaming Input
Using streaming input will convert your application into a streaming application build on top of Spark Structured Streaming.

To enable all other writers and also enable multiple outputs for a single streaming dataframe, add ```batchMode``` to your job configuration, this will enable the [foreachBatch](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#using-foreach-and-foreachbatch) mode (only available in spark >= 2.4.0)
Check out all possible streaming configurations in the ```streaming``` section of the [sample job configuration file](config/job_config_sample.yaml).

Please note the following while using streaming applications:

* Multiple streaming aggregations (i.e. a chain of aggregations on a streaming DF) are not yet supported on streaming Datasets.

* Limit and take first N rows are not supported on streaming Datasets.
* Distinct operations on streaming Datasets are not supported.

* Sorting operations are supported on streaming Datasets only after an aggregation and in Complete Output Mode.

* Make sure to add the relevant [Output Mode](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#output-modes) to your Metric as seen in the Examples

* Make sure to add the relevant [Triggers](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#triggers) to your Metric if needed as seen in the Examples

* For more information please go to [Spark Structured Streaming WIKI](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)

#### Kafka Input
Kafka input allows reading messages from topics
```yaml
inputs:
  testStream:
    kafka:
      servers:
        - 127.0.0.1:9092
      topic: test
      consumerGroup: testConsumerGroupID # optional
      schemaRegistryUrl: https://schema-registry-url # optional
      schemaSubject: subject # optional
```
When using kafka input, writing is only available to ```File``` and ```Kafka```, and only to a single output.
* In order to measure your consumer lag you can use the ```consumerGroup``` parameter to track your application offsets against your kafka input.
This will commit the offsets to kafka, as a new dummy consumer group.

* we use ABRiS as a provided jar In order to deserialize your kafka stream messages (https://github.com/AbsaOSS/ABRiS), add the  ```schemaRegistryUrl``` option to the kafka input config
spark-submit command should look like so:

```spark-submit --repositories http://packages.confluent.io/maven/ --jars https://repo1.maven.org/maven2/za/co/absa/abris_2.11/3.1.1/abris_2.11-3.1.1.jar --packages org.apache.spark:spark-avro_2.11:2.4.4,org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.4,io.confluent:kafka-schema-registry-client:5.3.0,io.confluent:kafka-avro-serializer:5.3.0 --class com.yotpo.metorikku.Metorikku metorikku.jar```
 
* If your subject schema name is not ```<TOPIC NAME>-value``` (e.g. if the topic is a regex pattern) you can specify the schema subject in the ```schemaSubject``` section

###### Topic Pattern
Kafka input also allows reading messages from multiple topics by using subscribe pattern:
```yaml
inputs:
  testStream:
    kafka:
      servers:
        - 127.0.0.1:9092
      # topicPattern can be any Java regex string
      topicPattern: my_topics_regex.*
      consumerGroup: testConsumerGroupID # optional
      schemaRegistryUrl: https://schema-registry-url # optional
      schemaSubject: subject # optional
```
* While using topicPattern, consider using ```schemaRegistryUrl``` and ```schemaSubject``` in case your topics have different schemas.

##### File Streaming Input
Metorikku supports streaming over a file system as well.
You can use the Data stream reading by specifying ```isStream: true```, 
and a specific path in the job (must be a single path) as a streaming source, this will trigger jobs for new files added to the folder.
```yaml
inputs:
  testStream:
    file:
      path: examples/file_input_stream/input
      isStream: true
      format: json
      options:
        timestampFormat: "yyyy-MM-dd'T'HH:mm:ss'Z'"
```

##### Watermark
Metorikku supports Watermark method which helps a stream processing engine to deal with late data.
You can use watermarking by adding a new udf step in your metric:
```yaml
# This will become the new watermarked dataframe name.
- dataFrameName: dataframe
  classpath: com.yotpo.metorikku.code.steps.Watermark
  params:
    # Watermark table my_table
    table: my_table
    # The column representing the event time (needs to be a TIMESTAMP or DATE column)
    eventTime: event
    delayThreshold: 2 hours
```

#### Instrumentation
One of the most useful features in Metorikku is it's instrumentation capabilities.

Instrumentation metrics are written by default to what's configured in [spark-metrics](https://spark.apache.org/docs/latest/monitoring.html#metrics).

Metorikku sends automatically on top of what spark is already sending the following:

* Number of rows written to each output

* Number of successful steps per metric

* Number of failed steps per metric
 
* In streaming: records per second

* In streaming: number of processed records in batch

You can also send any information you like to the instrumentation output within a metric.
by default the last column of the schema will be the field value. 
Other columns that are not value or time columns will be merged together as the name of the metric. 
If writing directly to influxDB these will become tags.

Check out the [example](examples/movies_metric.yaml) for further details.

##### using InfluxDB

You can also send metric directly to InfluxDB (gaining the ability to use tags and time field).

Check out the [example](examples/influxdb) and also the [InfluxDB E2E test](e2e/influxdb) for further details.

##### Elasticsearch output
Elasticsearch output allows bulk writing to elasticsearch
We use elasticsearch-hadoop as a provided jar - spark-submit command should look like so:

```spark-submit --packages org.elasticsearch:elasticsearch-hadoop:6.6.1 --class com.yotpo.metorikku.Metorikku metorikku.jar```

Check out the [example](examples/elasticsearch) and also the [Elasticsearch E2E test](e2e/elasticsearch) for further details.

#### Docker
Metorikku is provided with a [docker image](https://hub.docker.com/r/metorikku/metorikku).

You can use this docker to deploy metorikku in container based environments (we're using [Nomad by HashiCorp](https://www.nomadproject.io/)).

Check out this [docker-compose](docker/docker-compose.yml) for a full example of all the different parameters available and how to set up a cluster.

Currently the image only supports running metorikku in a spark cluster mode with the standalone scheduler.

The image can also be used to run E2E tests of a metorikku job.
Check out an example of running a kafka 2 kafka E2E with docker-compose [here](e2e/kafka/docker-compose.yml)

#### UDF
Metorikku supports adding custom code as a step.
This requires creating a JAR with the custom code.
Check out the [UDF examples directory](examples/udf) for a very simple example of such a JAR.

The only thing important in this JAR is that you have an object with the following method:
```scala
object SomeObject {
  def run(ss: org.apache.spark.sql.SparkSession, metricName: String, dataFrameName: String, params: Option[Map[String, String]]): Unit = {}
}
```
Inside the run function do whatever you feel like, in the example folder you'll see that we registered a new UDF.
Once you have a proper scala file and a ```build.sbt``` file you can run ```sbt package``` to create the JAR.

When you have the newly created JAR (should be in the target folder), copy it to the spark cluster (you can of course also deploy it to your favorite repo).

You must now include this JAR in your spark-submit command by using the ```--jars``` flag, or if you're using java to run add it to the ```-cp``` flag.

Now all that's left is to add it as a new step in your metric:
```yaml
- dataFrameName: dataframe
  classpath: com.example.SomeObject
  params:
    param1: value1
```
This will trigger your ```run``` method with the above dataFrameName.
Check out the built-in code steps [here](src/main/scala/com/yotpo/metorikku/code/steps). 

*NOTE: If you added some dependencies to your custom JAR build.sbt you have to either use [sbt-assembly](https://github.com/sbt/sbt-assembly) to add them to the JAR or you can use the ```--packages``` when running the spark-submit command* 

#### Apache Hive metastore
Metorikku supports reading and saving tables with Apache hive metastore.
To enable hive support via spark-submit (assuming you're using MySQL as Hive's DB but any backend can work) send the following configurations:
```bash
spark-submit \
--packages mysql:mysql-connector-java:5.1.75 \
--conf spark.sql.catalogImplementation=hive \
--conf spark.hadoop.javax.jdo.option.ConnectionURL="jdbc:mysql://localhost:3306/hive?useSSL=false&createDatabaseIfNotExist=true" \
--conf spark.hadoop.javax.jdo.option.ConnectionDriverName=com.mysql.jdbc.Driver \
--conf spark.hadoop.javax.jdo.option.ConnectionUserName=user \
--conf spark.hadoop.javax.jdo.option.ConnectionPassword=pass \
--conf spark.sql.warehouse.dir=/warehouse ...
```

*NOTE: If you're running via the standalone metorikku you can use system properties instead (```-Dspark.hadoop...```) and you must add the MySQL connector JAR to your class path via ```-cp```*

This will enable reading from the metastore.

To write an external table to the metastore you need to add **tableName** to your output configuration:
```yaml
...
output:
- dataFrameName: moviesWithRatings
  outputType: Parquet
  outputOptions:
    saveMode: Overwrite
    path: moviesWithRatings.parquet
    tableName: hiveTable
    overwrite: true
```
Only file formats are supported for table saves (**Parquet**, **CSV**, **JSON**).

To write a managed table (that will reside in the warehouse dir) simply omit the **path** in the output configuration.

To change the default database you can add the following to the job configuration:
```yaml
...
catalog:
  database: some_database
...

```

Check out the [examples](examples/hive) and the [E2E test](e2e/hive)


#### Apache Hudi
Metorikku supports reading/writing with [Apache Hudi](https://github.com/apache/incubator-hudi).

Hudi is a very exciting project that basically allows upserts and deletes directly on top of partitioned parquet data.

In order to use Hudi with Metorikku you need to add to your classpath (via ```--jars``` or if running locally with ```-cp```) 
an external JAR from here: https://repo1.maven.org/maven2/org/apache/hudi/hudi-spark-bundle/0.5.0-incubating/hudi-spark-bundle-0.5.0-incubating.jar

To run Hudi jobs you also have to make sure you have the following spark configuration (pass with ```--conf``` or ```-D```):
```properties
spark.serializer=org.apache.spark.serializer.KryoSerializer
```

After that you can start using the new Hudi writer like this:

#### Job config
```yaml
output:
  hudi:
    dir: /examples/output
    # This controls the level of parallelism of hudi writing (should be similar to shuffle partitions)
    parallelism: 1
    # upsert/insert/bulkinsert
    operation: upsert
    # COPY_ON_WRITE/MERGE_ON_READ
    storageType: COPY_ON_WRITE
    # Maximum number of versions to retain 
    maxVersions: 1
    # Hive database to use when writing
    hiveDB: default
    # Hive server URL
    hiveJDBCURL: jdbc:hive2://hive:10000
    hiveUserName: root
    hivePassword: pass
```

#### Metric config
```yaml
dataFrameName: test
  outputType: Hudi
  outputOptions:
    path: test.parquet
    # The key to use for upserts
    keyColumn: userkey
    # This will be used to determine which row should prevail (newer timestamps will win)
    timeColumn: ts
    # Partition column - note that hudi support a single column only, so if you require multiple levels of partitioning you need to add / to your column values
    partitionBy: date
    # Mapping of the above partitions to hive (for example if above is yyyy/MM/dd than the mapping should be year,month,day)
    hivePartitions: year,month,day
    # Hive table to save the results to
    tableName: test_table
    # Add missing columns according to previous schema, if exists
    alignToPreviousSchema: true
    # Remove completely null columns
    removeNullColumns: true
```

In order to delete send in your dataframe a boolean column called ```_hoodie_delete```, if it's true that row will be deleted.

Check out the [examples](e2e/hudi) and the [E2E test](e2e/hudi) for more details.

Also check the full list of configurations possible with hudi [here](http://hudi.incubator.apache.org/configurations.html).

#### Apache Atlas
Metorikku supports Data Lineage and Governance using [Apache Atlas](https://atlas.apache.org/) and the [Spark Atlas Connector](https://github.com/hortonworks-spark/spark-atlas-connector) 

Atlas is an open source Data Governance and Metadata framework for Hadoop which provides open metadata management and governance capabilities for organizations to build a catalog of their data assets, classify and govern these assets and provide collaboration capabilities around these data assets for data scientists, analysts and the data governance team.

In order to use the spark-atlas-connector with Metorikku  you need to add to your classpath (via ```--jars``` or if running locally with ```-cp```) 
an external JAR from here: https://github.com/YotpoLtd/spark-atlas-connector/releases/download/latest/spark-atlas-connector-assembly.jar

To integrate the connector with Metorikku docker, you need to pass `USE_ATLAS=true` as en environment variable and the following config will be automatically added to `spark-default.conf`:
```properties
spark.extraListeners=com.hortonworks.spark.atlas.SparkAtlasEventTracker
spark.sql.queryExecutionListeners=com.hortonworks.spark.atlas.SparkAtlasEventTracker
spark.sql.streaming.streamingQueryListeners=com.hortonworks.spark.atlas.SparkAtlasStreamingQueryEventTracker
```
For a full example please refer to examples/docker-compose-atlas.yml
## License  
See the [LICENSE](LICENSE.md) file for license rights and limitations (MIT).
