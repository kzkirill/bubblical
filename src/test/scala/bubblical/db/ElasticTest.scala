package bubblical.db

import bubblical.config.SparkConfLocal
import bubblical.config.SparkLocal.conf
import org.apache.spark.sql.SparkSession
import org.elasticsearch.spark._

import org.scalatest.FunSuite

/**
  * Created by kirill on 14/08/17.
  */
class ElasticTest extends FunSuite{
  val conf = SparkConfLocal.conf
//        .set("es.nodes","search-bubbling-dev1-27mxzc6n6wqfco4mqeqnteideu.eu-west-1.es.amazonaws.com")
        .set("es.resource", "bubbling-dev1/test1")
        .set("es.index.auto.create", "true")
  val scSession = SparkSession.builder().config(conf) getOrCreate()
  val sc = scSession.sparkContext

  test("Connect ans write to Elastic"){
    val numbers = Map("one" -> 1, "two" -> 2, "three" -> 3)
    val airports = Map("arrival" -> "Otopeni", "SFO" -> "San Fran")

    val rdd = sc.makeRDD(Seq(numbers, airports))
    rdd.take(10).foreach(println(_))
    rdd.saveToEs("bubbling-dev1/test1")
  }


}
