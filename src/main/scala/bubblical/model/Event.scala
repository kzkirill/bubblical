package bubblical.model

import java.sql.Timestamp

import org.apache.spark.sql.Row

/**
  * Created by Kirill on 2/25/2017.
  */
case class Event(time: Long, userID: String, action: String, duration: Double)

object Event {
  val timePosition = 0
  val userIDPosition = 1
  val actionPosition = 2
  val durationPosition = 3

  def apply(row: Row) = {
    val dbTime: Timestamp = row(timePosition).asInstanceOf[Timestamp]
    new Event(dbTime.getTime, row(userIDPosition).asInstanceOf[String], row(actionPosition).asInstanceOf[String], row(durationPosition).asInstanceOf[Double])
  }
}
