package bubblical.db

import java.sql.Timestamp

import bubblical.config.{SparkConfLocal, SparkLocalElastic}
import bubblical.config.SparkLocal.conf
import bubblical.model.{AggregatedListKey, SessionAggregated, SessionsAggregatedList}
import org.apache.spark.sql.SparkSession
import org.elasticsearch.spark._
import org.scalatest.FunSuite

/**
  * Created by kirill on 14/08/17.
  */
class ElasticTest extends FunSuite{
/*
  val conf = SparkConfLocal.conf
//        .set("es.nodes","search-bubbling-dev1-27mxzc6n6wqfco4mqeqnteideu.eu-west-1.es.amazonaws.com")
        .set("es.resource", "bubbling-dev1/test1")
        .set("es.index.auto.create", "true")
  val scSession = SparkSession.builder().config(conf) getOrCreate()
*/
  val scSession = SparkLocalElastic.scSession
  scSession.conf.set("es.resource", "bubbling-dev1/test1")
  val sc = scSession.sparkContext

  test("Connect ans write to Elastic"){
    val numbers = Map("one" -> 1, "two" -> 2, "three" -> 3)
    val airports = Map("arrival" -> "Otopeni", "SFO" -> "San Fran")
    val maps = Seq(numbers, airports)
    println("Scala data structure " + maps)
    val rdd = sc.makeRDD(maps)
    rdd.take(10).foreach(println(_))
    rdd.saveToEs("bubbling-dev1/test1")
  }

  val list1 = Seq(
    SessionAggregated(Timestamp.valueOf("2017-02-14 13:30:00.0"),"sphone",3561740732192401l,434.629d,144.87633333333335d),
    SessionAggregated(Timestamp.valueOf("2017-02-14 13:45:00.0"),"sphone",3561740732192401l,434.629,144.87633333333335),
    SessionAggregated(Timestamp.valueOf("2017-02-14 14:00:00.0"),"sphone",3561740732192401l,134.8145d,67.40725d),
    SessionAggregated(Timestamp.valueOf("2017-02-14 14:30:00.0"),"sphone",3561740732192401l,440.2344d,440.2344d),
    SessionAggregated(Timestamp.valueOf("2017-02-14 15:00:00.0"),"sphone",3561740732192401l,722.7031d,722.7031d),
    SessionAggregated(Timestamp.valueOf("2017-02-14 15:30:00.0"),"sphone",3561740732192401l,121.9824d,40.6608d)
  )
  val list2 = Seq(
    SessionAggregated(Timestamp.valueOf("2017-02-14 15:00:00.0"),"sphone",8672580223070220l,370684.21d,370684.21d),
    SessionAggregated(Timestamp.valueOf("2017-02-14 15:15:00.0"),"sphone",8672580223070220l,370684.21d,370684.21d)
  )
  test("Write case classes"){
    val key1 = new AggregatedListKey("sphone",3561740732192401l)
    val key2 = new AggregatedListKey("sphone",8672580223070220l)
    val values1 = Seq(SessionsAggregatedList(key1, list1),SessionsAggregatedList(key2, list2))
    val rdd = sc.parallelize(values1)
    rdd.collect().foreach(println(_))

    rdd.saveToEs("bubbling-dev1/aggregation2")
    //http://localhost:9200/bubbling-dev1/aggregation2/_search?q=8672580223070220
    val readRDD = sc.esRDD("bubbling-dev1/aggregation2")
    readRDD.collect().foreach(println(_))
  }

  test("Write case and _ids"){
    val id1 = "sphone" + 3561740732192401l
    val id2 = "sphone" + 8672580223070220l
    val key1 = new AggregatedListKey("sphone",3561740732192401l)
    val key2 = new AggregatedListKey("sphone",8672580223070220l)

    val values1 = Seq((id1 -> SessionsAggregatedList(key1, list1)),(id2 -> SessionsAggregatedList(key2, list2)))
    val rdd = sc.parallelize(values1)
    rdd.collect().foreach(println(_))

    rdd.saveToEsWithMeta("bubbling-dev1/aggregationIds")
    //http://localhost:9200/bubbling-dev1/aggregationIds/_search?q=8672580223070220
    //http://localhost:9200/bubbling-dev1/aggregationIds/sphone8672580223070220
    val readRDD = sc.esRDD("bubbling-dev1/aggregationIds")
    readRDD.collect().foreach(println(_))
  }

  test("Pair RDD examples"){
    val otp = Map("iata" -> "OTP", "name" -> "Otopeni")
    val muc = Map("iata" -> "MUC", "name" -> "Munich")
    val sfo = Map("iata" -> "SFO", "name" -> "San Fran")

    val airportsRDD = sc.makeRDD(Seq((1, otp), (2, muc), (3, sfo)))
    airportsRDD.collect.foreach(println(_))
    airportsRDD.saveToEsWithMeta("airports/2015")
    //http://localhost:9200/airports/2015/1
  }


}
