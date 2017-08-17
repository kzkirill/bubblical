package bubblical.model

import java.sql.Timestamp
import java.time.LocalDateTime

import org.apache.spark.sql.Row

/**
  * Created by kirill on 01/07/17.
  */
case class SessionAggregated(stopTime: Timestamp,APN:String,imei: Long,DownLoadKBSum: Double,DownLoadKBAvg: Double)
case class AggregatedListKey(apn:String,imei:Long)
case class SessionsAggregatedList(key:AggregatedListKey,list:Seq[SessionAggregated])

object SessionAggregated{
  def apply(row:Row): SessionAggregated ={
    val stopTime:Timestamp = row.getAs[Timestamp]("stop_time")
    val apn:String = row.getAs[String]("APN")
    val imei: Long = row.getAs[Long]("imei")
    val dlKBSum: Double = row.getAs[Double]("DOWNLOAD_KBSum")
    val dlKBAvg: Double = row.getAs[Double]("DOWNLOAD_KBAvg")
    new SessionAggregated(stopTime,apn,imei,dlKBSum,dlKBAvg)
  }
}

object SessionsAggregatedList{
  def apply(key:AggregatedListKey,oneEntry:SessionAggregated):SessionsAggregatedList = new SessionsAggregatedList(key,Seq(oneEntry))
}
object AggregatedListKey{
  def apply(dataEntry:SessionAggregated) = new AggregatedListKey(dataEntry.APN,dataEntry.imei)
}

object utils {
  def produceId(dataEntry: SessionAggregated) = dataEntry.APN + dataEntry.imei
  def reduceFun(entry1: SessionsAggregatedList,entry2: SessionsAggregatedList) = SessionsAggregatedList(entry1.key, entry1.list ++ entry2.list )
}

