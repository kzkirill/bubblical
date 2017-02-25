import bubblical.service.Events
import org.scalatest.FunSuite

/**
  * Created by Kirill on 2/25/2017.
  */
class EventserviceTest extends FunSuite {
  val tested = new Events {}

  test("Events service returns Map") {
    println(tested.aggregated)
  }
}
