package bubblical.model

import java.time.LocalDateTime

import org.apache.spark.sql.Row

/**
  * Created by kirill on 01/07/17.
  */
case class SessionAggregated(stopTime: LocalDateTime,APN:String,imei: Long,DownLoadKBSum: Double,DownLoadKBAvg: Double)

object SessionAggregated{
//  def apply(stopTime: LocalDateTime,APN:String,imei: Long,DownLoadKBSum: Double,DownLoadKBAvg: Double) = new SessionAggregated(stopTime,APN,imei,DownLoadKBSum,DownLoadKBAvg)
  def apply(row:Row): SessionAggregated ={
    val stopTime:LocalDateTime = row.getAs[LocalDateTime]("stop_time")
    val apn:String = row.getAs[String]("APN")
    val imei: Long = row.getAs[Long]("imei")
    val dlKBSum: Double = row.getAs[Double]("DOWNLOAD_KBSum")
    val dlKBAvg: Double = row.getAs[Double]("DOWNLOAD_KBAvg")
    new SessionAggregated(stopTime,apn,imei,dlKBSum,dlKBAvg)
  }
}
