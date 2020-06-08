package com.tristanpenman.chordial.dht

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.ws.{Message, TextMessage}
import akka.http.scaladsl.server.Directives.{get, handleWebSocketMessages, path}
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Flow
import ch.megard.akka.http.cors.scaladsl.CorsDirectives.cors
import spray.json.DefaultJsonProtocol

import scala.concurrent.Await
import scala.concurrent.duration.Duration

object Dht extends App with DefaultJsonProtocol {
  implicit val system = ActorSystem("Service")
  implicit val mat = ActorMaterializer()
  implicit val ec = system.dispatcher

  val dhtService: Flow[Message, Message, _] = Flow[Message].map {
    case TextMessage.Strict(txt) => TextMessage("ECHO: " + txt)
    case _                       => TextMessage("Message type unsupported")
  }

  def route = cors() {
    path("ws") {
      get {
        handleWebSocketMessages(dhtService)
      }
    }
  }

  Http().bindAndHandle(route, "0.0.0.0", 4567)

  Await.result(system.whenTerminated, Duration.Inf)
}
