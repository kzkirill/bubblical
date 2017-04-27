package bubblical.db

import bubblical.config.Context.config
import com.datastax.spark.connector.writer.CassandraRowWriter.Factory
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite

/**
  * Created by kirill on 22/04/17.
  */
case class Aggregation(apn: String, imei: String, download_average: Double, download_total: Double)

class CassandraAccessTest extends FunSuite {
  /*
    val sparkSession = SparkLocal.scSession
    implicit val spark = sparkSession
    implicit val sc = sparkSession.sparkContext
  */
  val url = config.getString("cassandra.url")
  val sparkConf: SparkConf = new SparkConf().setAppName("Spark-cassandra-bubblical").setMaster("local[4]").set("spark.cassandra.connection.host", url)

  val spark = SparkSession.builder().config(sparkConf).getOrCreate()

  implicit val sc = spark.sparkContext

  implicit val rowWriterFactory = Factory

  test("Cassandra save") {
    val tableName = "aggregations"
    val tested = new CassandraAccess
    val dataRDD = spark.sparkContext.parallelize(Seq(Aggregation("internetg", "3515830709328501", 1140.9648, 285.2412)))
    tested.create(dataRDD, tableName)

    /*
        val collection = sc.parallelize(Seq(("cat", 30), ("fox", 40)))
        collection.saveToCassandra("test", "words", SomeColumns("word", "count"))
    */
  }

}
