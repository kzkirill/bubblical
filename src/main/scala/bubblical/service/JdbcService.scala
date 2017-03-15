package bubblical.service

import bubblical.config.SparkLocal
import bubblical.db.JDBCAccess
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

import scala.reflect.ClassTag


/**
  * Created by kirill on 03/03/17.
  */
sealed class JdbcService[T: ClassTag](val tableName: String, val rowMapper: (Row => T)) extends Serializable with RDDProvider[T] {
  val sparkSession = SparkLocal.scSession
  val dbDriver = JDBCAccess

  def read: RDD[T] = {
//    implicit val encoder = Encoders.kryo[T]
    dbDriver.read(sparkSession, tableName).createOrReplaceTempView(tableName)
    val sqlDF = sparkSession.sql(s"select * from $tableName ")
    sqlDF.show
    sqlDF.printSchema
    sqlDF.rdd.map(rowMapper)
  }

}
