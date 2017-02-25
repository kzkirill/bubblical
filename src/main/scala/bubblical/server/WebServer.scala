package bubblical.server

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.server.Directives._
import akka.stream.ActorMaterializer
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import bubblical.controller.EventsController
import spray.json.DefaultJsonProtocol._

import scala.concurrent.Future
import scala.io.StdIn

/**
  * Created by Kirill on 2/6/2017.
  */
object WebServer {

  case class Prediction(list: List[String])

  implicit val predictionsFormat = jsonFormat1(Prediction)

  def main(args: Array[String]) = {
    implicit val system = ActorSystem()
    implicit val materialize = ActorMaterializer()
    implicit val executionContext = system.dispatcher

    val eventsCtlr = new EventsController()

    val route: Route = {
      get {
        pathPrefix("aggregate" / Segment) { prefix =>
          val eventsOption: Future[Option[Map[String, Double]]] = Future {
            Some(eventsCtlr aggregate)
          }

          onSuccess(eventsOption) {
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
