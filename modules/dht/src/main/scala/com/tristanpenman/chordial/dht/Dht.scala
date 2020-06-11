package com.tristanpenman.chordial.dht

import java.util.Properties

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.ws.{Message, TextMessage}
import akka.http.scaladsl.server.Directives.{get, handleWebSocketMessages, path}
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Flow
import ch.megard.akka.http.cors.scaladsl.CorsDirectives.cors
import com.typesafe.config.ConfigFactory
import spray.json.DefaultJsonProtocol

import scala.concurrent.Await
import scala.concurrent.duration.Duration

object Dht extends App with DefaultJsonProtocol {
  val usage = """
    Usage: dht [--seed-node address] [--virtual-nodes num]
  """

  if (args.length == 0) {
    println(usage)
    System.exit(0)
  }

  type OptionMap = Map[Symbol, Any]

  @scala.annotation.tailrec
  def consumeOptions(map: OptionMap, list: List[String]): OptionMap =
    list match {
      case Nil => map
      case "--dht-port" :: value :: tail =>
        consumeOptions(map ++ Map(Symbol("dht-port") -> value), tail)
      case "--seed-node" :: value :: tail =>
        consumeOptions(map ++ Map(Symbol("seed-node") -> value), tail)
      case "--virtual-nodes" :: value :: tail =>
        consumeOptions(map ++ Map(Symbol("virtual-nodes") -> value.toInt), tail)
      case option :: _ =>
        println("Unknown option " + option)
        throw new Exception("Unknown option " + option)
    }

  // Parse command line arguments
  val options = consumeOptions(Map(), args.toList)
  println(options)

  // Override default akka configuration
  val properties = new Properties()
  options.get(Symbol("dht-port")) match {
    case Some(x: String) =>
      properties.setProperty("akka.remote.netty.tcp.port", x)
    case _ =>
  }

  // Set up an actor system for the DHT backend
  var config = ConfigFactory.parseProperties(properties)
  implicit val system = ActorSystem("Service", config.withFallback(ConfigFactory.load()))
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
