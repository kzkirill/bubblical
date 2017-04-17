package bubblical.service

import org.apache.spark.sql.DataFrame

/**
  * Created by kirill on 17/04/17.
  */
trait DFProvider {
  def read: DataFrame
}
