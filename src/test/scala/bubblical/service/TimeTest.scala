package bubblical.service

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import org.scalatest.FunSuite

/**
  * Created by kirill on 07/03/17.
  */
class TimeTest extends FunSuite {
  test("Use time") {
    val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm")

    val timeFromString = LocalDateTime.parse("2017-02-14 07:02", formatter)
    println(timeFromString)
  }

}
