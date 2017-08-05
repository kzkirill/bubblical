import bubblical.config.SparkLocal
import bubblical.service.{JdbcService, Sessions}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * Created by kirill on 05/08/17.
  */
object OnLocalMachine {
  val sparkSession = SparkLocal.scSession
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
      println(key)
      values.foreach{ one =>
        print("   ")
        println(one)
      }
    }
  }


}
