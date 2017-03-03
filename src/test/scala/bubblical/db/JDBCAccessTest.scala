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
    val tableName = "small_session_stage"
    testTable(tableName)


  }

  test("jdbc access test table session") {
    val tableName = "session"
    testTable(tableName)
  }

  private def testTable(tableName: String) = {
    dbDriver.read(sparkSession, tableName).createOrReplaceTempView(tableName)
    val sqlDF = sparkSession.sql(s"SELECT * FROM $tableName")
    sqlDF.show
  }
}
