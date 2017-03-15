package bubblical.service

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import bubblical.model.Session

/**
  * Created by kirill on 03/03/17.
  */

sealed class Sessions(val dsProvider: RDDProvider[Session]) {

  val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm")
  val earliestDate = LocalDateTime.parse("2017-02-14 07:02", formatter)

//  def this() = {
//    this(new JdbcService("session"))
//  }
//
  def aggregate: Map[String, Double] = {
    val reduced = dsProvider.read
    Map("as" -> 45.89)
  }

}

//object Sessions {
//  def apply() = new Sessions
//}
