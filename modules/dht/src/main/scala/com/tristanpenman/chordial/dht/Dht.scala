package com.tristanpenman.chordial.dht

import java.util.Properties

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.ws.{Message, TextMessage}
import akka.http.scaladsl.server.Directives.{get, handleWebSocketMessages, path}
import akka.pattern.ask
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Flow
import akka.util.Timeout
import ch.megard.akka.http.cors.scaladsl.CorsDirectives.cors
import com.tristanpenman.chordial.core.Node
import com.tristanpenman.chordial.core.Node._
import com.typesafe.config.ConfigFactory
import spray.json.DefaultJsonProtocol

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.Random

object Dht extends App with DefaultJsonProtocol {
  val usage = """
    Usage: dht [--dht-hostname host] [--dht-port port] [--seed-node address] [--virtual-nodes num]
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
      case "--dht-hostname" :: value :: tail =>
        consumeOptions(map ++ Map(Symbol("dht-hostname") -> value), tail)
      case "--dht-port" :: value :: tail =>
        consumeOptions(map ++ Map(Symbol("dht-port") -> value), tail)
      case "--seed-node" :: value :: tail =>
        consumeOptions(map ++ Map(Symbol("seed-node") -> value), tail)
      case "--service-port" :: value :: tail =>
        consumeOptions(map ++ Map(Symbol("service-port") -> value.toInt), tail)
      case "--virtual-nodes" :: value :: tail =>
        consumeOptions(map ++ Map(Symbol("virtual-nodes") -> value.toInt), tail)
      case option :: _ =>
        println("Unknown option " + option)
        throw new Exception("Unknown option " + option)
    }

  // Parse command line arguments
  val options = consumeOptions(Map(), args.toList)
  println(options)

  // Number of virtual nodes
  val virtualNodes = options.get(Symbol("virtual-nodes")) match {
    case Some(x: Int) if x > 0 =>
      x
    case None =>
      1
    case _ =>
      throw new Exception("Number of virtual nodes must be at least 1")
  }

  // Override default akka configuration
  val properties = new Properties()
  options.get(Symbol("dht-hostname")) match {
    case Some(x: String) =>
      properties.setProperty("akka.remote.netty.tcp.hostname", x)
    case _ =>
  }
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

  private val keyspaceBits = 6

  private val idModulus = 1 << keyspaceBits

  // How long Node should wait until an algorithm is considered to have timed out. This should be significantly
  // longer than the external request timeout, as some algorithms will make multiple external requests before
  // running to completion
  private val algorithmTimeout = Timeout(5000.milliseconds)

  // How long to wait when making requests that may be routed to other nodes
  private val externalRequestTimeout = Timeout(500.milliseconds)

  val firstNode = system.actorOf(
    Node.props(Random.nextInt(idModulus), keyspaceBits, algorithmTimeout, externalRequestTimeout, system.eventStream))

  options.get(Symbol("seed-node")) match {
    case Some(x: String) =>
      firstNode
        .ask(GetSeedId(x))(algorithmTimeout)
        .mapTo[GetSeedIdResponse]
        .map {
          case GetSeedIdOk(id) =>
            println(id)
          case GetSeedIdError(message) =>
            println(message)
        }
        .recover {
          case exception =>
            println(exception.getMessage)
        }
    case _ =>
  }

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

  Http().bindAndHandle(route, "0.0.0.0", options.getOrElse(Symbol("service-port"), "4567").toString.toInt)

  Await.result(system.whenTerminated, Duration.Inf)
}
