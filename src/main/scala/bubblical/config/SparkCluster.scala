package bubblical.config

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * Created by kirill on 05/08/17.
  */
object SparkCluster {
  private val conf = new SparkConf()
    .setMaster("spark://52.16.180.162:7077")
    .setAppName("bubblical")
    .set("spark.cores.max", "10")
  val scSession = SparkSession.builder().config(conf) getOrCreate()

}
