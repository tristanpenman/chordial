package com.tristanpenman.chordial.dht

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.ws.{Message, TextMessage}
import akka.http.scaladsl.server.Directives.{get, handleWebSocketMessages, path}
import akka.stream.scaladsl.Flow
import ch.megard.akka.http.cors.scaladsl.CorsDirectives.cors
import com.google.common.net.HostAndPort
import spray.json.DefaultJsonProtocol

import scala.concurrent.Await
import scala.concurrent.duration._

object Dht extends App with DefaultJsonProtocol {
  val usage =
    """
    Usage: dht [options]

    Valid options:
      --http-interface <interface>  Interface on which to listen for HTTP connections (default is 0.0.0.0)
      --http-port <port>            TCP port on which to listen for HTTP connections (default is 8080)
      --node-interface <interface>  Interface on which to listen for messages from other nodes (default is 0.0.0.0)
      --node-port <port>            UDP port on which to listen for messages from other nodes (default is random)
      --seed-node <hostname:port>   Optional hostname and port for a seed node to join an existing network
  """

  //
  // Parse command line arguments
  //

  type OptionMap = Map[Symbol, String]

  @scala.annotation.tailrec
  def consumeOptions(map: OptionMap, list: List[String]): OptionMap =
    list match {
      case Nil => map
      case "--help" :: _ | "-h" :: _ =>
        println(usage)
        System.exit(0)
        map
      case "--http-interface" :: value :: tail =>
        consumeOptions(map ++ Map(Symbol("http-interface") -> value), tail)
      case "--http-port" :: value :: tail =>
        consumeOptions(map ++ Map(Symbol("service-port") -> value), tail)
      case "--node-interface" :: value :: tail =>
        consumeOptions(map ++ Map(Symbol("node-interface") -> value), tail)
      case "--node-port" :: value :: tail =>
        consumeOptions(map ++ Map(Symbol("node-port") -> value), tail)
      case "--seed-node" :: value :: tail =>
        consumeOptions(map ++ Map(Symbol("seed-node") -> value), tail)
      case option :: _ =>
        println("Unknown option " + option)
        throw new Exception("Unknown option " + option)
    }

  val options = consumeOptions(Map(), args.toList)

  //
  // Validate command line arguments or fallback to defaults
  //

  val httpInterface = options.getOrElse[String](Symbol("http-interface"), "localhost")
  val httpPort = options.getOrElse[String](Symbol("http-port"), "8080").toIntOption match {
    case Some(value) if value >= 0 =>
      value
    case _ =>
      throw new Exception("Invalid value for http-port")
  }

  val nodeInterface = options.getOrElse[String](Symbol("node-interface"), "localhost")
  val nodePort = options.getOrElse[String](Symbol("node-port"), "0").toIntOption match {
    case Some(value) if value >= 0 =>
      value
    case _ =>
      throw new Exception("Invalid value for node-port")
  }

  val seedNode = options
    .get(Symbol("seed-node"))
    .map(value => {
      try {
        //noinspection UnstableApiUsage
        val hostAndPort = HostAndPort.fromString(value)
        SeedNode(hostAndPort.getHost, hostAndPort.getPort)
      } catch {
        case _: IllegalArgumentException => throw new Exception("Invalid value for seed-node")
      }
    })

  //
  // Set up an actor system for the DHT backend
  //

  implicit val system: ActorSystem = ActorSystem("Service")

  //
  // Start coordinator actor
  //

  val keyspaceBits = 16

  val frontend = system.actorOf(
    Coordinator.props(
      keyspaceBits,
      nodeInterface,
      nodePort,
      seedNode
    ))

  //
  // Start web server for user queries
  //

  val wsService: Flow[Message, Message, _] = Flow[Message].map {
    case TextMessage.Strict(txt) => TextMessage("ECHO: " + txt)
    case _                       => TextMessage("Message type unsupported")
  }

  def route = cors() {
    path("ws") {
      get {
        handleWebSocketMessages(wsService)
      }
    }
  }

  Http().bindAndHandle(route, httpInterface, httpPort)

  Await.result(system.whenTerminated, Duration.Inf)
}
