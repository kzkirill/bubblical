package bubblical.service

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import org.scalatest.FunSuite


/**
  * Created by kirill on 07/03/17.
  */
class TimeTest extends FunSuite {
  test("Use time") {
    import bubblical.model.truncateToQuarter

    val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm")

    val timeFromString1 = LocalDateTime.parse("2017-02-14 07:02", formatter)
    val timeFromString1Quarter = LocalDateTime.parse("2017-02-14 07:00", formatter)
    val timeFromString2 = LocalDateTime.parse("2017-02-14 07:22", formatter)
    val timeFromString2Quarter = LocalDateTime.parse("2017-02-14 07:15", formatter)
    val timeFromString3 = LocalDateTime.parse("2017-02-14 07:29", formatter)
    val timeFromString3Quarter = LocalDateTime.parse("2017-02-14 07:15", formatter)

    assert(truncateToQuarter(timeFromString1) == timeFromString1Quarter)
    assert(truncateToQuarter(timeFromString2) == timeFromString2Quarter)
    assert(truncateToQuarter(timeFromString3) == timeFromString3Quarter)
  }

}
