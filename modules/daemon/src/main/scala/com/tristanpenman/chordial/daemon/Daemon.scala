package com.tristanpenman.chordial.daemon

import akka.actor.{ActorSystem, Props}
import akka.io.IO
import akka.pattern.ask
import akka.util.Timeout
import spray.can.Http

import com.tristanpenman.chordial.core.Node
import com.tristanpenman.chordial.daemon.service.ServiceActor

import scala.concurrent.duration._

object Daemon extends App {
  implicit val system = ActorSystem("chordial-daemon")

  private val nodeId = 0L
  private val nodeRef = system.actorOf(Props(classOf[Node], nodeId))
  private val service = system.actorOf(Props(classOf[ServiceActor], nodeId, nodeRef))

  implicit val timeout = Timeout(5.seconds)

  IO(Http) ? Http.Bind(service, interface = "localhost", port = 0)
}
