package bubblical.db

import com.datastax.spark.connector._
import com.datastax.spark.connector.writer.RowWriterFactory
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.util.Try

/**
  * Created by kirill on 22/04/17.
  */

class CassandraAccess(implicit spark: SparkContext) extends Serializable {
  import bubblical.config.Context.config

  val sparkCassandraUrlPropertyName = "spark.cassandra.connection.host"

  val keySpacename: String = config.getString("cassandra.keyspace")
  val url = config.getString("cassandra.url")

//  spark.getConf.set(sparkCassandraUrlPropertyName, url)

  def create[T](rdd: RDD[T], tableName: String)(implicit rwf: RowWriterFactory[T]) = {
    rdd.saveToCassandra(keySpacename, tableName)
  }

  def retrieve(tableName: String, whereClause: String = ""): Option[Array[CassandraRow]] = {
    Try(spark.cassandraTable(keySpacename, tableName).where(whereClause).collect()).toOption
  }

}
