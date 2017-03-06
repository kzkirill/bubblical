package bubblical.service

import bubblical.model.Session

/**
  * Created by kirill on 03/03/17.
  */

sealed class Sessions(val rddProvider: RDDProvider[Session]) {

  def this() = {
    this(new JdbcService("session", Session))
  }

  def aggregate: Map[String,Double]= {
    val reduced = rddProvider.read map (session => (session.ipAddress, session.downloadKb)) reduceByKey ((a, b) => a + b)
    (reduced collect) toMap
  }

}

object Sessions {
  def apply() = new Sessions
}
