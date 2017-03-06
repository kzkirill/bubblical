package bubblical.service

import org.apache.spark.rdd.RDD

/**
  * Created by kirill on 03/03/17.
  */
trait RDDProvider[T] {
  def read: RDD[T]
}
