package bubblical.model


import java.time._
import java.time.temporal.ChronoUnit

import org.apache.spark.sql.Row

/**
  * Created by Kirill on 3/2/2017.
  */
/*root
|-- ne_id: integer (nullable = true)
|-- lastupdatetime: timestamp (nullable = true)
|-- session_id: string (nullable = true)
|-- start_time: timestamp (nullable = true)
|-- stop_time: timestamp (nullable = true)
|-- UPLOAD_KB: double (nullable = true)
|-- DOWNLOAD_KB: double (nullable = true)
|-- TOTAL_KB: double (nullable = true)
|-- APN: string (nullable = true)
|-- RAT: string (nullable = true)
|-- imei: long (nullable = true)
|-- ip_address: string (nullable = true)
|-- CID: integer (nullable = true)
|-- mcc: integer (nullable = true)
|-- mnc: integer (nullable = true)*/



case class Session(neid: Int,
                   lastUpdateTime: Option[LocalDateTime],
                   sessionid: String,
                   startTime: Option[LocalDateTime],
                   stopTime: Option[LocalDateTime],
                   uploadKb: Double,
                   downloadKb: Double,
                   totalKb: Double,
                   apn: String,
                   rat: String,
                   imei: Long,
                   ipAddress: String,
                   cid: Int,
                   mcc: Int,
                   mnc: Int,
                   lastUpdateTimeToQuarter: Option[LocalDateTime],
                   lastUpdateTimeToHour: Option[LocalDateTime]) {

}

object Session extends Rowappliable[Session] {
  val neidPos = 0
  val lastupdatetimePos = 1
  val sessionidPos = 2
  val startTimePos = 3
  val stoptimePOS = 4
  val uploadKbPos = 5
  val downloadKbPos = 6
  val totalKbPos = 7
  val apnPos = 8
  val ratPos = 9
  val imeiPos = 10
  val ipAddressPos = 11
  val cidPos = 12
  val mccPos = 13
  val mncPos = 14

  def apply(row: Row) = {
    val lastUpdateTime = convertTimestamp(row, lastupdatetimePos)
    val startTime = convertTimestamp(row, startTimePos)
    val stopTime = convertTimestamp(row, stoptimePOS)

    new Session(row.getInt(neidPos),
      lastUpdateTime,
      row.getString(sessionidPos),
      startTime,
      stopTime,
      row.getDouble(uploadKbPos),
      row.getDouble(downloadKbPos),
      row.getDouble(totalKbPos),
      row.getString(apnPos),
      row.getString(ratPos),
      row.getLong(imeiPos),
      row.getString(ipAddressPos),
      row.getInt(cidPos),
      row.getInt(mccPos),
      row.getInt(mncPos),
      lastUpdateTime map (truncateToQuarter(_)),
      lastUpdateTime map (_.truncatedTo(ChronoUnit.HOURS)))
  }
}



