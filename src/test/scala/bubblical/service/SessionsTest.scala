package bubblical.service

import java.sql.Timestamp
import java.time.LocalDateTime

import bubblical.config.SparkLocal
import bubblical.model.SessionAggregated
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
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

//    implicit val encoder1 = Encoders.kryo[SessionAggregated]
//    implicit val encoder2 = Encoders.kryo[LocalDateTime]

    val service = Sessions(JdbcService("session"))
    val aggregated = service.aggregate(List("APN", "imei"))
//    aggregated show(100)
    val aggregatedDS = aggregated map(row => {
      val data = SessionAggregated(row)
      ((data.APN, data.imei) -> List(data))
    })

    val keyValues: RDD[((String, Long), List[SessionAggregated])] = aggregatedDS.rdd.reduceByKey((entry1,entry2) => entry1 ++ entry2 )
    keyValues
      .take(5).foreach{one =>
      val key = one._1
      val values = one._2
      println(key)
      values.foreach{ one =>
        print("   ")
        println(one)
      }
    }
  }

  test("RDD reduce"){
    val foo = spark.sparkContext.parallelize(List((1,3),(1,5),(1,8),(2,4)))
    foo.groupByKey.foreach(println(_))
    foo.reduceByKey((i,j)=> i+j).collect().foreach(println(_))
  }
}
