package bubblical.controller

import bubblical.service.Events

/**
  * Created by Kirill on 2/25/2017.
  */
class EventsController {
  val service = new Events {}

  def aggregate = {
    service.aggregated
  }

}
