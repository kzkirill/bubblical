package bubblical.db

import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite

/**
  * Created by Kirill on 2/18/2017.
  */
class JDBCAccessTest extends FunSuite {
  test("jdbc access test") {
    val spark = SparkSession.builder().appName("JDBC acces driver test").master("local[4]") getOrCreate()
    val dbDriver = JDBCAccess

    val df = dbDriver.read(spark)

    df show

  }
}
