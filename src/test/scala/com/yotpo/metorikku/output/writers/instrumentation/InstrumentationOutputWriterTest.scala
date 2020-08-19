package com.yotpo.metorikku.output.writers.instrumentation

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.yotpo.metorikku.instrumentation.{InstrumentationFactory, InstrumentationProvider}
import org.scalatest.FunSuite
import org.scalatest.mockito.MockitoSugar.mock


class InstrumentationOutputWriterTest extends FunSuite with DataFrameSuiteBase{
  val states = Map("AL" -> "Alabama", "AK" -> "Alaska")
//  val factory = mock[InstrumentationFactory]
  val factory = InstrumentationProvider.getInstrumentationFactory(Some("foo"), None)
  val writer = new InstrumentationOutputWriter(props = states, dataFrameName = "df", metricName = "metric", factory)

  import sqlContext.implicits._
  val propsData = Seq(
    ("month_ts", 1),
    ("number_of_ratings", 2)
  )

  val df = propsData.toDF("timeColumn", "valueColumns")
  writer.write(df)
}
