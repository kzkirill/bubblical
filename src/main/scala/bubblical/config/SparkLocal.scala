package bubblical.config

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Kirill on 2/25/2017.
  */
object SparkLocal {
  private val conf = new SparkConf()
    .setMaster("local[*]")
    .setAppName("bubblical")
  val scSession = SparkSession.builder().config(conf) getOrCreate()
//  def sc = new scSession.sparkContext
}
