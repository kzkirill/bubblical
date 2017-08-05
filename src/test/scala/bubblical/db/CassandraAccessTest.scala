package bubblical.db

import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.FunSuite

/**
  * Created by kirill on 22/04/17.
  */
case class Aggregation(apn: String, imei: String, download_average: Double, download_total: Double)

class CassandraAccessTest extends FunSuite {
  val url = "52.215.24.146"//config.getString("cassandra.url")
  val port = "9160"
//  val port = "9042"

  val conf = new SparkConf(true)
    .set("spark.cassandra.connection.host", url)
    .set("spark.cassandra.connection.port", port)
    .set("spark.cassandra.auth.username", "cassandra")
    .set("spark.cassandra.auth.password", "cassandra")

  val sc = new SparkContext("local[*]", "test", conf)

  test("Cassandra access") {
    val tableName = "aggregations"
    val keySpace = "bubbling_dev_test_1"

    implicit val sContext = sc

    implicit val connector = CassandraConnector(conf)

    val rdd = sc.cassandraTable(keySpace,tableName)
    rdd.collect().foreach(println(_))

  }

}
