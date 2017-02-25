package bubblical.db

import bubblical.config.SparkLocal
import bubblical.model.Event
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
}
