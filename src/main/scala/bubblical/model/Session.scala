package bubblical.model

import org.apache.spark.sql.Row


/**
  * Created by Kirill on 3/2/2017.
  */
case class Session(neid: Int,
                   lastUpdateTime: Option[Long],
                   sessionid: String,
                   startTime: Option[Long],
                   stopTime: Option[Long],
                   uploadKb: Double,
                   downloadKb: Double,
                   totalKb: Double,
                   apn: String,
                   rat: String,
                   imei: Long,
                   ipAddress: String,
                   cid: Int,
                   mcc: Int,
                   mnc: Int) {

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
      row.getInt(mncPos))

  }
}


