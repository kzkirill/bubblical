package bubblical

import org.apache.spark.sql.Row
//import org.scala_tools.time.Imports._

import java.time._
/**
  * Created by kirill on 03/03/17.
  */
package object model {

  def convertTimestamp(row: Row, index: Int): Option[LocalDateTime]  = {
    val dbValue = row.getTimestamp(index)
    if (null == dbValue) None else Some(dbValue.toLocalDateTime)
  }

}
