package com.yotpo.metorikku.output.writers.instrumentation

import com.yotpo.metorikku.exceptions.MetorikkuWriteFailedException
import com.yotpo.metorikku.instrumentation.InstrumentationFactory
import com.yotpo.metorikku.output.Writer
import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.sql.{DataFrame, Row}


class InstrumentationOutputWriter(props: Map[String, Any],
                                  dataFrameName: String,
                                  metricName: String,
                                  instrumentationFactory: InstrumentationFactory) extends Writer {
  @transient lazy val log: Logger = LogManager.getLogger(this.getClass)

  val valueColumnProperty: Option[String] = Some(Option(props).getOrElse(Map()).get("valueColumn").toString)
  val timeColumnProperty: Option[String] = Some(Option(props).getOrElse(Map()).get("timeColumn").toString)
  val valueColumnsProperty: Option[List[String]] = Some(Option(props).getOrElse(Map()).get("valueColumns").toList.map(_.toString))

  // scalastyle:off cyclomatic.complexity
  override def write(dataFrame: DataFrame): Unit = {
    val columns = dataFrame.schema.fields.zipWithIndex
    val indexOfValCol: Option[Int]= valueColumnProperty.flatMap(col => Option(dataFrame.schema.fieldNames.indexOf(col)))
    val indexOfTimeCol = timeColumnProperty.flatMap(col => Option(dataFrame.schema.fieldNames.indexOf(col)))
    val valueColumnsIndices: Option[List[Int]]= valueColumnsProperty.map(col => Option(dataFrame.schema.fieldNames.indexOf(col)).toList)

    log.info(s"Starting to write Instrumentation of data frame: $dataFrameName on metric: $metricName")
    dataFrame.foreachPartition(p => {
      val client = instrumentationFactory.create()

      // use last column if valueColumn is missing and valueColoumns is missing
      // ToDO: NEEDS TO BE REMOVED
      val actualIndexOfValCol = indexOfValCol.getOrElse(columns.length - 1)

      val actualIndicesOfValCol: List[Int] = (indexOfValCol, valueColumnsIndices) match {
        case (Some(_), None) => List(indexOfValCol.get)
        case (None, Some(_)) => valueColumnsIndices.get
        case _ => List(columns.length - 1)
      }

      p.foreach(row => {
        try {

          val tags = Map("metric" -> metricName, "dataframe" -> dataFrameName) ++
            columns.filter {
              case (column, index) => !actualIndicesOfValCol.contains(index) &&
                (!indexOfTimeCol.isDefined || index != indexOfTimeCol.get) //replace index with indices
            }.map {
              case (column, index) =>
                column.name -> row.get(index).asInstanceOf[AnyVal].toString
            }.toMap

          val time = getTime(indexOfTimeCol, row)
          val metricValue = row.get(actualIndexOfValCol)

          if (metricValue != null && classOf[Number].isAssignableFrom(metricValue.getClass)) {
            val longValue = metricValue.asInstanceOf[Number].longValue()
            val valueColumnName = row.schema.fieldNames(actualIndexOfValCol)
            //client.gauge(name = valueColumnName, value = longValue, tags = tags,  time = time)
            client.gauge2(fields = tags, tags = tags,  time = time)
          }

        }catch {
          case ex: Throwable =>
            throw MetorikkuWriteFailedException(s"failed to write instrumentation on data frame: $dataFrameName " +
              s"for row: ${row.toString()}", ex)
        }
      })
      client.close()
    })
  }
  // scalastyle:on cyclomatic.complexity
  def getTime(indexOfTimeCol: Option[Int], row: Row): Long = {
    if (indexOfTimeCol.isDefined) {
      if (!row.isNullAt(indexOfTimeCol.get)) {
        val timeColValue = row.get(indexOfTimeCol.get)
        if (!classOf[Number].isAssignableFrom(timeColValue.getClass)) {
          throw MetorikkuWriteFailedException("Defined time column is not a number")
        }
        return row.get(indexOfTimeCol.get).asInstanceOf[Number].longValue()
      } else {
        throw MetorikkuWriteFailedException("Defined time column is null for row")
      }
    }
    System.currentTimeMillis()
  }
}
