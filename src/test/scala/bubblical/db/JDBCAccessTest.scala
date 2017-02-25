package bubblical.db

import bubblical.config.SparkLocal
import bubblical.model.Event
import org.apache.spark.sql.SQLContext
import org.scalatest.FunSuite


/**
  * Created by Kirill on 2/18/2017.
  */
class JDBCAccessTest extends FunSuite {
  val sparkSession = SparkLocal.scSession
  val dbDriver = JDBCAccess

  test("jdbc access test table Events") {

    dbDriver.read(sparkSession, "Events").createOrReplaceTempView("Events")
    val sqlDF = sparkSession.sql("select * from Events where action <> 'read'")
    sqlDF.show

    val rddEvent = sqlDF.rdd map (row => Event(row))

    rddEvent foreach println

  }

  test("jdbc access test table small_session") {
    val tableName = "small_session"
    dbDriver.read(sparkSession, tableName).createOrReplaceTempView(tableName)
    val sqlDF = sparkSession.sql("SELECT * FROM small_session")
    sqlDF.show


  }

}
