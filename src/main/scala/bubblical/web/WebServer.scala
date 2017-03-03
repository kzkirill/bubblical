package bubblical.web

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives.{complete, get, onSuccess, pathPrefix}
import akka.http.scaladsl.server.Route
import akka.stream.ActorMaterializer
import bubblical.service.Sessions

import scala.concurrent.Future
import scala.io.StdIn

/**
  * Created by Kirill on 2/6/2017.
  */
object WebServer {

  def main(args: Array[String]) = {
    implicit val system = ActorSystem()
    implicit val materialize = ActorMaterializer()
    implicit val executionContext = system.dispatcher

    val sessions = Sessions()

    val route: Route = {
      get {
        pathPrefix("aggregate" ) {
          val sessionOption: Future[Option[String]] = Future {
//            Some(sessions aggregate)
            Some("adad")
          }

          onSuccess(sessionOption) {
            case Some(item) => complete(item)
            case None => complete(StatusCodes.NotFound)
          }
        }
      }

    }

    val bindingFuture = Http().bindAndHandle(route, "localhost", 8080)
    println("server online at http:\\localhost:8080\nPress RETURN to stop")
    StdIn.readLine()
    bindingFuture.
      flatMap(_.unbind()).
      onComplete(_ => {
        system.terminate()
        println("Actor system terminated")
      }
      )
  }

}
