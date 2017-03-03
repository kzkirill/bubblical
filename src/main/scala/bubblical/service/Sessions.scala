package bubblical.service

import bubblical.model.Session

/**
  * Created by kirill on 03/03/17.
  */

class Sessions extends JdbcService("session", Session) {
  def aggregate: Map[String, Double] = {
    val reduced = read map (session => (session.ipAddress, session.downloadKb)) reduceByKey ((a, b) => a + b)
    (reduced collect) toMap

  }

}

object Sessions {
  def apply() = new Sessions
}
