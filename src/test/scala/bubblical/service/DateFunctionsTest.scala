package bubblical.service

import java.sql.Timestamp
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import bubblical.SparkTestContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.scalatest.FunSuite

/**
  * Created by kirill on 29/04/17.
  */
case class WithDate(date: Timestamp, id: Int)

case class Processed(id: Int, quarterHour: Int)

class DateFunctionsTest extends FunSuite with SparkTestContext {

  val datePattern = "yyyy-MM-dd HH:mm"
  val formatter = DateTimeFormatter.ofPattern(datePattern)


  test("Truncate date function") {
    import spark.implicits._
    import truncatedColumnsNames._
    def testOne(resultDF: DataFrame, id: Int, truncatedExpression: String, expected: String) = {
      val quarterForID = resultDF.select(expr(truncatedExpression)).where($"id".equalTo(id))
      assert(quarterForID.take(1)(0).getTimestamp(0) == Timestamp.valueOf(LocalDateTime.parse(expected, formatter)))
    }


    val date1 = Timestamp.valueOf(LocalDateTime.parse("2017-02-14 07:02", formatter))
    val date2 = Timestamp.valueOf(LocalDateTime.parse("2017-02-14 07:22", formatter))
    val date3 = Timestamp.valueOf(LocalDateTime.parse("2017-02-14 07:30", formatter))
    val date4 = Timestamp.valueOf(LocalDateTime.parse("2017-02-14 08:31", formatter))
    val date5 = Timestamp.valueOf(LocalDateTime.parse("2017-02-14 07:52", formatter))


    val dateColumnName = "date"
    val to15MinColumn = nearestQuarterHourColumn(dateColumnName, datePattern)

    val value1 = new WithDate(date1, 1)
    val df = Seq(value1, new WithDate(date2, 2), new WithDate(date3, 3), new WithDate(date4, 4), new WithDate(date5, 5)).toDF

    val quarterResult = df.select($"id", column(dateColumnName), to15MinColumn)
    val expectedQuartes = Map(1 -> "2017-02-14 07:00",
      2 -> "2017-02-14 07:15",
      3 -> "2017-02-14 07:30",
      4 -> "2017-02-14 08:30",
      5 -> "2017-02-14 07:45")

    expectedQuartes.keys.map(key => testOne(quarterResult, key, nearestQuarterHour, expectedQuartes(key)))

    val halfResult = df.select($"id", column(dateColumnName), nearestHalfHourColumn(dateColumnName, datePattern))
    val expectedHalfes = Map(1 -> "2017-02-14 07:00",
      2 -> "2017-02-14 07:00",
      3 -> "2017-02-14 07:30",
      4 -> "2017-02-14 08:30",
      5 -> "2017-02-14 07:30")

    expectedHalfes.keys.map(key => testOne(halfResult, key, nearestHalfHour, expectedHalfes(key)))

  }

}
