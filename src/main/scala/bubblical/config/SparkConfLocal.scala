package bubblical.config

import org.apache.spark.SparkConf

/**
  * Created by kirill on 14/08/17.
  */
object SparkConfLocal {
  val conf = new SparkConf()
    .setMaster("local[*]")
    .setAppName("bubblical")
}
