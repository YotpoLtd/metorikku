# Metorikku

[![Build Status](https://travis-ci.com/YotpoLtd/metorikku.svg?token=vzfSdSxstLDATEkdJ2zY&branch=master)](https://travis-ci.com/YotpoLtd/metorikku)
[![codecov](https://codecov.io/gh/YotpoLtd/metorikku/branch/master/graph/badge.svg?token=OCpytlDExS)](https://codecov.io/gh/YotpoLtd/metorikku)

Metorikku is a library that simplifies writing and executing ETLs on top of [Apache Spark](http://spark.apache.org/).
A user needs to write a simple JSON configuration file that includes SQL queries and run Metorikku on a spark cluster.
The platform also includes a way to write tests for metrics using MetorikkuTester.

![Metorikku Logo](logo/metorikku.png)

### Getting started
To run Metorikku you must first define 2 files.

##### MQL file
An MQL (Metorikku Query Language) file defines the steps and queries of the ETL as well as where and what to output.

For example a simple configuration JSON should be as follows:
```json
{
    "steps": [
        {
            "sql": "SELECT * from table_1 where id > 100",
            "dataFrameName": "df1"
        },
        {
            "sql": "SELECT * from df1 where id < 1000",
            "dataFrameName": "df2"   
        }
    ],
    "output": [
        {
            "dataFrameName": "df2",
            "outputType": "Parquet",
            "outputOptions":
            {
                "saveMode": "Overwrite",
                "path": "df2.parquet"
            }
        }
    ]
}
```
Take a look at the [examples file](https://github.com/YotpoLtd/metorikku/blob/master/examples) for further configuration examples.

##### Run configuration file
Metorikku uses a YAML file to describe the run configuration.
This file will include **input sources**, **output destinations** and the location of the **metric config** files.

So for example a simple config.yaml file should be as follows:
```yaml
metrics:
  - configuration_directory
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
**CSV, JSON, parquet**

And the following outputs:
**CSV, JSON, parquet, Redshift, Cassandra, Segment**<br />
Redshift - s3_access_key and s3_secret are supported from spark-submit

### Running Metorikku
There are currently 3 options to run Metorikku.
##### Run on a spark cluster
*To run on a cluster Metorikku requires [Apache Spark](http://spark.apache.org/) v2.2+*
* Download the [last released JAR](https://github.com/YotpoLtd/metorikku/releases/latest)
* Run the following command:
     `spark-submit --class com.yotpo.metorikku.Metorikku metorikku-assembly-0.0.3.jar -c config.yaml`

##### Run locally
*Metorikku is released with a JAR that includes a bundled spark.*
* Download the [last released TEST JAR](https://github.com/YotpoLtd/metorikku/releases/latest)
* Run the following command:
`java -cp metorikku-assembly-test-0.0.3.jar com.yotpo.metorikku.Metorikku -c config.yaml`

##### Run as a library
*It's also possible to use Metorikku inside your own software*
*Metorikku library requires scala 2.11*
* Add the following dependency to your build.sbt:
`"com.yotpo" % "metorikku" % "0.0.3"`
* Start Metorikku by creating an instance of `com.yotpo.metorikku.config` and run `com.yotpo.metorikku.Metorikku.execute(config)`


#### Metorikku Tester
In order to test and fully automate the deployment of MQLs (Metorikku query language files) we added a method to run tests against MQLs.

A test is comprised of 2 files:
##### Test settings
This defines what to test and where to get the mocked data.
For example, a simple `test_settings.json` file will be:
```json
{
  "metric": "/path/to/metric"
  "mocks": [
    {
      "name": "table_1",
      "path": "mocks/table_1.jsonl"
    }
  ],
  "tests": {
    "df2": [
      {
        "id": 200,
        "name": "test"
      },
      {
        "id": 300,
        "name": "test2"
      }
    ]
  }
}
```

And the corresponding `mocks/table_1.jsonl`:
```json
{ "id": 200, "name": "test" }
{ "id": 300, "name": "test2" }
{ "id": 1, "name": "test3" }
```

##### Running Metorikku Tester
You can run Metorikku tester in any of the above methods (just like a normal Metorikku).
The main class changes from `com.yotpo.metorikku.Metorikku` to `com.yotpo.metorikku.MetorikkuTester`

## License  
See the [LICENSE](LICENSE.md) file for license rights and limitations (MIT).
