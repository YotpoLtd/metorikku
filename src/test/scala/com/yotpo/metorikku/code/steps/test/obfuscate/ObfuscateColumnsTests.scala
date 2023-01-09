package com.yotpo.metorikku.code.steps.test.obfuscate

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.roundeights.hasher.Implicits._
import com.yotpo.metorikku.code.steps.obfuscate.{
  ColumnsNotPartOfOriginalSchemaException,
  ObfuscateColumns
}
import com.yotpo.metorikku.exceptions.MetorikkuException
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.matchers.must.Matchers._
import org.scalatest.funspec.AnyFunSpec

class ObfuscateColumnsTests extends AnyFunSpec with BeforeAndAfterEach with DataFrameSuiteBase {
  private var sparkSession: SparkSession = _
  private val defaultSchema = StructType(
    List(
      StructField("col1", IntegerType, true),
      StructField("col2", StringType, true),
      StructField("col3", StringType, true),
      StructField("col4", StringType, true),
      StructField("col5", StringType, true)
    )
  )
  private val defaultData = Seq(
    Row(1, "1", "2", "3", "4"),
    Row(2, "1", "2", "3", "4"),
    Row(3, "1", "2", "3", "4")
  )

  override def beforeEach() {
    sparkSession = SparkSession
      .builder()
      .appName("udf tests")
      .master("local")
      .config("", "")
      .getOrCreate()
  }

  override def afterEach() {
    spark.stop()
  }

  private def createDataFrame(
      data: Seq[Row] = defaultData,
      schema: StructType = defaultSchema
  ): DataFrame = {
    val rdd = sparkSession.sparkContext.parallelize(data)
    sparkSession.createDataFrame(rdd, schema)
  }

  describe("""Obfuscates columns in a dataframe.""") {
    describe("""
        | run - the main method - obfuscates the columns and creates (or replace) view.
        |
        | receives:
        |   ss - SparkSession.
        |   metricName - not used but forced in order to run.
        |   dataFrameName - the view name.
        |   params - should contain the following:
        |     columns - columns to obfuscate - represented by a string separated by a delimiter.
        |     delimiter - how to split the columns string.
        |     value - how to obfuscate, could be a literal value and a func: [md5 | sha256].
        |     table - dataframe source.
        """.stripMargin) {
      describe("When columns isn't supplied to params") {
        it("throws a MetorikkuException.") {
          val params = Some(
            Map(
              "delimiter" -> ",",
              "value"     -> "*********",
              "table"     -> "table"
            )
          )

          a[MetorikkuException] must be thrownBy (ObfuscateColumns.run(
            sparkSession,
            "metricName",
            "dataframe",
            params
          ))
        }
      }

      describe("When delimiter isn't supplied to params") {
        it("throws a MetorikkuException.") {
          val params = Some(
            Map(
              "columns" -> "id,name",
              "value"   -> "*********",
              "table"   -> "table"
            )
          )

          a[MetorikkuException] must be thrownBy (ObfuscateColumns.run(
            sparkSession,
            "metricName",
            "dataframe",
            params
          ))
        }
      }

      describe("When value isn't supplied to params") {
        it("throws a MetorikkuException.") {
          val params = Some(
            Map(
              "columns"   -> "id,name",
              "delimiter" -> ",",
              "table"     -> "table"
            )
          )

          a[MetorikkuException] must be thrownBy (ObfuscateColumns.run(
            sparkSession,
            "metricName",
            "dataframe",
            params
          ))
        }
      }

      describe("When table isn't supplied to params") {
        it("throws a MetorikkuException.") {
          val params = Some(
            Map(
              "columns"   -> "id,name",
              "delimiter" -> ",",
              "value"     -> "md5"
            )
          )

          a[MetorikkuException] must be thrownBy (ObfuscateColumns.run(
            sparkSession,
            "metricName",
            "dataframe",
            params
          ))
        }
      }

      it("Obfuscates the columns and creates a view according to the table param.") {
        val df    = createDataFrame()
        val table = "table"
        val value = "********"
        df.createOrReplaceTempView(table)

        val params = Some(
          Map(
            "columns"   -> "col2,col3",
            "delimiter" -> ",",
            "value"     -> value,
            "table"     -> table
          )
        )
        val dataFrameName = "dataFrameName"

        ObfuscateColumns.run(sparkSession, "metricName", dataFrameName, params)

        val actualDf = sparkSession.table(dataFrameName)
        val expectedDf = {
          val data = Seq(
            Row(1, value, value, "3", "4"),
            Row(2, value, value, "3", "4"),
            Row(3, value, value, "3", "4")
          )

          val rdd = sparkSession.sparkContext.parallelize(data)
          sparkSession.createDataFrame(rdd, df.schema)
        }

        assertDataFrameEquals(actualDf, expectedDf)
      }
    }
    describe("""
        | obfuscateColumns - obfuscate columns in a dataframe.
        |
        | receives:
        |   df.
        |   columns - Array of columns to obfuscate.
        |   value - ['md5' | 'sha256' | literal_value].
        |""".stripMargin) {
      describe("When one or more of the columns aren't part of the dataframe schema") {
        it("throws a ColumnsAreNotPartOfTheOriginalSchemaException.") {
          val df      = createDataFrame()
          val columns = Array("non_existing_column")

          an[ColumnsNotPartOfOriginalSchemaException] must be thrownBy (ObfuscateColumns
            .obfuscateColumns(df, columns, "value"))
        }
      }

      describe("When a column that's being obfuscated has null values") {
        it("doesn't apply the obfuscation on the null values.") {
          val df = {
            createDataFrame(
              data = Seq(
                Row(1, "1", "2", "3", "4"),
                // scalastyle:off null
                Row(2, "1", null, "3", "4")
                // scalastyle:on null
              )
            )
          }
          val columns = Array("col2", "col3")
          val value   = "********"

          val actualDf = ObfuscateColumns.obfuscateColumns(df, columns, value)
          val expectedDf = {
            val data = Seq(
              Row(1, value, value, "3", "4"),
              // scalastyle:off null
              Row(2, value, null, "3", "4")
              // scalastyle:on null
            )

            val rdd = sparkSession.sparkContext.parallelize(data)
            sparkSession.createDataFrame(rdd, df.schema)
          }

          assertDataFrameEquals(actualDf, expectedDf)
        }
      }

      describe("When value is a literal") {
        it("replaces all the columns values with the literal") {
          val df      = createDataFrame()
          val columns = Array("col2", "col3")
          val value   = "********"

          val actualDf = ObfuscateColumns.obfuscateColumns(df, columns, value)
          val expectedDf = {
            val data = Seq(
              Row(1, value, value, "3", "4"),
              Row(2, value, value, "3", "4"),
              Row(3, value, value, "3", "4")
            )

            val rdd = sparkSession.sparkContext.parallelize(data)
            sparkSession.createDataFrame(rdd, df.schema)
          }

          assertDataFrameEquals(actualDf, expectedDf)
        }
      }

      describe("When value is 'md5'") {
        it("applies md5 on the values of the columns") {
          val df      = createDataFrame()
          val columns = Array("col2", "col3")

          val actualDf = ObfuscateColumns.obfuscateColumns(df, columns, "md5")
          val expectedDf = {
            val data = Seq(
              Row(1, "1".md5.hex, "2".md5.hex, "3", "4"),
              Row(2, "1".md5.hex, "2".md5.hex, "3", "4"),
              Row(3, "1".md5.hex, "2".md5.hex, "3", "4")
            )

            val rdd = sparkSession.sparkContext.parallelize(data)
            sparkSession.createDataFrame(rdd, df.schema)
          }

          assertDataFrameEquals(actualDf, expectedDf)
        }
      }

      describe("When value is 'sha256'") {
        it("applies sha256 on the values of the columns") {
          val df      = createDataFrame()
          val columns = Array("col2", "col3")

          val actualDf = ObfuscateColumns.obfuscateColumns(df, columns, "sha256")
          val expectedDf = {
            val data = Seq(
              Row(1, "1".sha256.hex, "2".sha256.hex, "3", "4"),
              Row(2, "1".sha256.hex, "2".sha256.hex, "3", "4"),
              Row(3, "1".sha256.hex, "2".sha256.hex, "3", "4")
            )

            val rdd = sparkSession.sparkContext.parallelize(data)
            sparkSession.createDataFrame(rdd, df.schema)
          }

          assertDataFrameEquals(actualDf, expectedDf)
        }
      }
    }
  }
}
