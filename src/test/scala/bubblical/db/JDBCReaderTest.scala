package bubblical.db

import bubblical.config.SparkLocal
import bubblical.service.{JdbcService, nearestQuarterHourColumn}
import org.apache.spark.sql.functions.{avg, column, sum}
import org.scalatest.FunSuite

/**
  * Created by kirill on 01/07/17.
  */
class JDBCReaderTest extends FunSuite{
  test("jdbc reader") {
    val sparkSession = SparkLocal.scSession
    import sparkSession.implicits._

    //    implicit val encoder = Encoders.kryo[Session]
    val datePattern = "yyyy-MM-dd HH:mm"

    val jdbcReader = new JdbcService("session")
    val sessionsDF = jdbcReader.read
    val downLoadColumn = column("DOWNLOAD_KB")
    val stopTimeCol = nearestQuarterHourColumn("stop_time",datePattern)
    val columnNames = List("APN","imei")
    val groupByColumns = columnNames.map(column(_)) ++ List(stopTimeCol)

    sessionsDF.printSchema()

    val aggregated = sessionsDF.select($"ne_id", $"session_id", $"UPLOAD_KB", downLoadColumn, $"TOTAL_KB", $"imei", $"ip_address", $"APN", stopTimeCol, $"stop_time").
      groupBy(groupByColumns: _*).
      agg(sum(downLoadColumn).as("TotalDownload"), avg(downLoadColumn).as("AverageDownload")).
      sort(groupByColumns: _*)

    aggregated show
  }
}
