package bubblical.service

import bubblical.config.SparkLocal
import bubblical.model.Session
import org.scalatest.FunSuite

/**
  * Created by kirill on 03/03/17.
  */
class SessionsTest extends FunSuite {
  val sparkSession = SparkLocal.scSession
  test("jdbc reader") {
    //    implicit val encoder = Encoders.kryo[Session]

    val jdbcReader = new JdbcService[Session]("session", Session.apply)
    val sessions = jdbcReader.read
    sessions take (5) foreach (session => println(session))

    sessions.map(session => (session.neid -> session.downloadKb))
      .reduceByKey(_ + _) take (5) foreach (println)
  }
}
