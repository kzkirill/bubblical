package bubblical

import org.apache.spark.sql.Row

/**
  * Created by kirill on 03/03/17.
  */
package object model {

  def convertTimestamp(row: Row, index: Int): Option[Long] = {
    val dbValue = row.getTimestamp(index)
    if (null == dbValue) None else Some(dbValue.getTime)
  }

}
