package bubblical.service

import java.time.LocalDateTime

import bubblical.config.SparkLocal
import bubblical.model.SessionAggregated
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.functions._
import org.scalatest.FunSuite

/**
  * Created by kirill on 03/03/17.
  */
class SessionsTest extends FunSuite {
  val sparkSession = SparkLocal.scSession
  implicit val spark = sparkSession

  val rootLogger = Logger.getRootLogger()
  rootLogger.setLevel(Level.ERROR)

  test ("Sessions service test"){
    import spark.implicits._

    implicit val encoder1 = Encoders.kryo[SessionAggregated]
    implicit val encoder2 = Encoders.kryo[LocalDateTime]

    val service = Sessions(JdbcService("session"))
    val aggregated = service.aggregate(List("APN", "imei"))
    aggregated show(100)
    val aggregatedRDD = aggregated map(row => SessionAggregated(row)) map (entry => ((entry.APN,entry.imei) -> entry))
    aggregatedRDD show 20
  }
}
