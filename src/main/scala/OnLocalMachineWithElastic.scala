import bubblical.config.{SparkLocal, SparkLocalElastic}
import bubblical.service.{JdbcService, Sessions}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.elasticsearch.spark._

/**
  * Created by kirill on 05/08/17.
  */
object OnLocalMachineWithElastic {
  val sparkSession = SparkLocalElastic.scSession
  sparkSession.conf.set("es.resource", "bubbling-dev1/test1")
  implicit val spark = sparkSession

  val rootLogger = Logger.getRootLogger()
  rootLogger.setLevel(Level.ERROR)



  def main(args: Array[String]): Unit = {

    val service = Sessions(JdbcService("session"))
    val aggregated = service.aggregate(List("APN", "imei"))
    val keyValues = service.reduce(aggregated)
    keyValues
      .take(5).foreach{one =>
      val key = one._1
      val values = one._2
      println(key + " " + values.key)
      values.list.foreach{ one =>
        print("   ")
        println(one)
      }
    }

    keyValues.saveToEsWithMeta("bubbling-dev1/aggregated")

  }


}
