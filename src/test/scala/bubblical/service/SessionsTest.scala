package bubblical.service

import org.scalatest.FunSuite

/**
  * Created by kirill on 03/03/17.
  */
class SessionsTest extends FunSuite{


  test("sessions") {
    val sessions = Sessions()

    sessions.aggregate.foreach(println)
  }

}
