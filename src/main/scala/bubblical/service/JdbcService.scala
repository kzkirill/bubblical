package bubblical.service

import bubblical.config.SparkLocal
import bubblical.db.JDBCAccess
import org.apache.spark.sql.DataFrame


/**
  * Created by kirill on 03/03/17.
  */
//[T: ClassTag]
sealed class JdbcService(val tableName: String) extends Serializable with DFProvider {
  val sparkSession = SparkLocal.scSession
  val dbDriver = JDBCAccess

  def read: DataFrame = {
    //    implicit val encoder = Encoders.kryo[T]
    dbDriver.read(sparkSession, tableName).createOrReplaceTempView(tableName)
    val sqlDF = sparkSession.sql(s"select * from $tableName ")
    sqlDF
  }

}

object JdbcService{
  def apply(tableName: String) = new JdbcService(tableName)
}
