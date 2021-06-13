package com.tristanpenman.chordial.demo

import akka.actor._
import akka.http.scaladsl.model.ws.{Message, TextMessage}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.stream.scaladsl.{Flow, Source}
import ch.megard.akka.http.cors.scaladsl.CorsDirectives._

import scala.concurrent.ExecutionContext

final class WebSocketWorker(governor: ActorRef, eventsSource: Source[TextMessage, _])(implicit val ec: ExecutionContext)
    extends WebService {

  def route: Route =
    cors() {
      routes(governor) ~
        pathEndOrSingleSlash {
          handleWebSocketMessages(Flow[Message].take(0).prepend(eventsSource)) ~
            getFromFile("modules/demo/src/main/resources/public/index.html") ~
            getFromResource("public/index.html")
        } ~
        getFromDirectory("modules/demo/src/main/resources/public") ~
        getFromResourceDirectory("public")
    }
}

object WebSocketWorker {
  def apply(nodeRef: ActorRef, eventsSource: Source[TextMessage, _])(implicit ec: ExecutionContext): Route =
    new WebSocketWorker(nodeRef, eventsSource).route
}
