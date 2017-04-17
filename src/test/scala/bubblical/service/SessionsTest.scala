package bubblical.service

import bubblical.config.SparkLocal
import org.apache.spark.sql.functions._
import org.scalatest.FunSuite

/**
  * Created by kirill on 03/03/17.
  */
class SessionsTest extends FunSuite {
  val sparkSession = SparkLocal.scSession
  implicit val spark = sparkSession

  import sparkSession.implicits._

  test("jdbc reader") {
    //    implicit val encoder = Encoders.kryo[Session]

    val jdbcReader = new JdbcService("session")
    val sessionsDF = jdbcReader.read
    val downLoadColumn = column("DOWNLOAD_KB")
    val columnNames = List("APN","imei")
    val groupByColumns = columnNames.map(column(_))
    val truncatedDate = expr("stop_time").

    val aggregated = sessionsDF.select($"ne_id", $"session_id", $"UPLOAD_KB", downLoadColumn, $"TOTAL_KB", $"imei", $"ip_address", $"APN").
      groupBy(groupByColumns: _*).
      agg(sum(downLoadColumn).as("TotalDownload"), avg(downLoadColumn).as("AverageDownload")).
      sort(groupByColumns: _*)

    aggregated show
  }

  test ("Sessions service test"){
    val service = new Sessions(new JdbcService("session"))

    service.aggregate.show
  }
}
