package bubblical.config

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * Created by Kirill on 2/25/2017.
  */
object SparkLocal {
  private val conf = SparkConfLocal.conf
  val scSession = SparkSession.builder().config(conf) getOrCreate()
}
