package bubblical

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._

/**
  * Created by kirill on 29/04/17.
  */
package object service {

  import truncatedColumnsNames._

  def nearestQuarterHourColumn(dateColumnName: String, datePattern: String = "yyyy-MM-dd HH:mm") = {
    nearestTruncatedColumn(dateColumnName, datePattern)(truncateTo15Minutes)
  }

  def nearestHalfHourColumn(dateColumnName: String, datePattern: String = "yyyy-MM-dd HH:mm") = {
    nearestTruncatedColumn(dateColumnName, datePattern)(truncateTo30Minutes)
  }

  def nearestTruncatedColumn(dateColumnName: String, datePattern: String = "yyyy-MM-dd HH:mm")(truncation: (Column, Column, String) => Column) = {
    val dateColumn = column(dateColumnName)
    val minutes = minute(dateColumn)
    truncation(minutes, dateColumn, datePattern)
  }

  private def truncateTo15Minutes(minutes: Column, dateColumn: Column, datePattern: String) = {
    val quarterHour = when(minutes < 15, 0).when(minutes < 30, 15).when(minutes < 45, 30).otherwise(45).as("quarterHour")
    calculateTruncation(dateColumn, minutes, quarterHour, nearestQuarterHour, datePattern)
  }

  private def truncateTo30Minutes(minutes: Column, dateColumn: Column, datePattern: String) = {
    val halfHour = when(minutes < 30, 0).otherwise(30).as("halfHour")
    calculateTruncation(dateColumn, minutes, halfHour, nearestHalfHour, datePattern)
  }

  private def calculateTruncation(dateColumn: Column, minutesColumn: Column, truncatedColumn: Column, name: String, datePattern: String) = {
    to_utc_timestamp(from_unixtime(unix_timestamp(dateColumn) - minutesColumn * 60 + truncatedColumn * 60, datePattern), datePattern).as(name)
  }

  object truncatedColumnsNames {
    val nearestQuarterHour = "nearestQuarterHour"
    val nearestHalfHour = "nearestHalfHour"
  }

}
