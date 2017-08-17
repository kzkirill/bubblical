package bubblical.config

import org.apache.spark.sql.SparkSession

/**
  * Created by Kirill on 2/25/2017.
  */
object SparkLocalElastic {
  private val conf = SparkConfLocal.conf
//    .set("es.resource", "bubbling-dev1/test1")
    .set("es.index.auto.create", "true")
  val scSession = SparkSession.builder().config(conf) getOrCreate()
}
