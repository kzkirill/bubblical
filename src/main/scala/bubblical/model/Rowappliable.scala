package bubblical.model

import org.apache.spark.sql.Row

/**
  * Created by kirill on 03/03/17.
  */
trait Rowappliable[A] {
  def apply(row: Row): A
}
