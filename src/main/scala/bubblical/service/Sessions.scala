package bubblical.service

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import org.apache.spark.sql.functions.{avg, column, sum}
import org.apache.spark.sql.{Dataset, Row, SparkSession}

/**
  * Created by kirill on 03/03/17.
  */

sealed class Sessions(val dsProvider: DFProvider) {

  val datePattern = "yyyy-MM-dd HH:mm"
  val formatter = DateTimeFormatter.ofPattern(datePattern)
  val earliestDate = LocalDateTime.parse("2017-02-14 07:02", formatter)

  def aggregate(groupColumnNames: List[String])(implicit sparkSession: SparkSession): Dataset[Row] = {

    val sessionsDF = dsProvider.read
    val aggColumnName = "DOWNLOAD_KB"
    val aggColumn = column(aggColumnName)
    val aggColumnSumName = aggColumnName + "Sum"
    val aggColumnSum = column(aggColumnSumName)
    val aggColumnAvgName = aggColumnName + "Avg"
    val aggColumnAvg = column(aggColumnAvgName)

    val timeColumnName = "stop_time"
    val timeColumn = column(timeColumnName)

    val quarterHourColumn = nearestQuarterHourColumn(timeColumnName, datePattern)
    val halfHourColumn = nearestHalfHourColumn(timeColumnName, datePattern)
    val groupByColumns = groupColumnNames.map(column(_))
    val sortColumns = groupByColumns ++ List(timeColumn )


//    sessionsDF.select("*").where(column("imei").equalTo(3532960611056901l)) show 20

    val aggregatedQuarterHour = sessionsDF.select(quarterHourColumn.as(timeColumnName) :: aggColumn.as(aggColumnSumName) :: aggColumn.as(aggColumnAvgName) :: groupByColumns: _*)
      .where(timeColumnName + " is not null")
      .groupBy(quarterHourColumn.as(timeColumnName) :: groupByColumns: _*)
      .agg(sum(aggColumnSum).as(aggColumnSumName), avg(aggColumnAvg).as(aggColumnAvgName))

    val aggregatedHalfHour = aggregatedQuarterHour.
      groupBy(halfHourColumn.as(timeColumnName) :: groupByColumns: _*).
      agg(sum(aggColumnSum).as(aggColumnSumName), avg(aggColumnAvg).as(aggColumnAvgName))

//    val aggregated = aggregatedQuarterHour
    val aggregated = aggregatedHalfHour
      .union(aggregatedQuarterHour)
      .dropDuplicates(timeColumnName :: groupColumnNames)
      .sort(sortColumns: _*)
    aggregated
  }

}

object Sessions{
  def apply(dsProvider: DFProvider): Sessions = new Sessions(dsProvider)
}
