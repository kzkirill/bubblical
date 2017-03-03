package bubblical.model

import org.apache.spark.sql.Row
import java.time._
import java.sql.Date

/**
  * Created by Kirill on 3/2/2017.
  */
class Session {
case class Session()

  object Session{
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
      val lastUpdateTime = row.getDate(lastUpdateTime).getTime
      new Session()
    }
  }
}
