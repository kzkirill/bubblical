package bubblical.service

import bubblical.config.SparkLocal
import bubblical.db.JDBCAccess
import bubblical.model.Event

/**
  * Created by Kirill on 2/25/2017.
  */
trait Events {
  val sparkSession = SparkLocal.scSession
  val dbDriver = JDBCAccess

  def aggregated: Map[String, Double] = {

    dbDriver.read(sparkSession, "Events").createOrReplaceTempView("Events")
    val sqlDF = sparkSession.sql("select * from Events where action <> 'read'")
    sqlDF.show

    val rddEvent = sqlDF.rdd map (row => Event(row))
    val zero: Double = 0.0
    val reduced = rddEvent map (event => (event.userID, event.duration)) reduceByKey ((a, b) => a + b)
    (reduced collect) toMap
  }

}
