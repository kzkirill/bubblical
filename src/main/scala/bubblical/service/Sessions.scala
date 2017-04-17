package bubblical.service

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import org.apache.spark.sql.functions.{avg, column, sum}
import org.apache.spark.sql.{Dataset, Row, SparkSession}

/**
  * Created by kirill on 03/03/17.
  */

sealed class Sessions(val dsProvider: DFProvider) {

  val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm")
  val earliestDate = LocalDateTime.parse("2017-02-14 07:02", formatter)

  def aggregate(implicit sparkSession: SparkSession): Dataset[Row] = {
    import sparkSession.implicits._

    val sessionsDF = dsProvider.read
    val downLoadColumn = column("DOWNLOAD_KB")
    val columnNames = List("APN", "imei")
    val groupByColumns = columnNames.map(column(_))

    val aggregated = sessionsDF.select($"ne_id", $"session_id", $"UPLOAD_KB", downLoadColumn, $"TOTAL_KB", $"imei", $"ip_address", $"APN").
      groupBy(groupByColumns: _*).
      agg(sum(downLoadColumn).as("TotalDownload"), avg(downLoadColumn).as("AverageDownload")).
      sort(groupByColumns: _*)

    aggregated
  }

}

//object Sessions {
//  def apply() = new Sessions
//}
