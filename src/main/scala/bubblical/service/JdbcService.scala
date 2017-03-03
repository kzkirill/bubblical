package bubblical.service

import bubblical.config.SparkLocal
import bubblical.db.JDBCAccess
import bubblical.model.Rowappliable
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

/**
  * Created by kirill on 03/03/17.
  */
abstract class JdbcService[T:ClassTag](val tableName: String, val model: Rowappliable[T]) extends Serializable{
  val sparkSession = SparkLocal.scSession
  val dbDriver = JDBCAccess

  def read: RDD[T] = {

    dbDriver.read(sparkSession, tableName).createOrReplaceTempView(tableName)
    val sqlDF = sparkSession.sql(s"select * from $tableName ")
    sqlDF.show

    sqlDF.rdd map (row => model(row))
  }

}
