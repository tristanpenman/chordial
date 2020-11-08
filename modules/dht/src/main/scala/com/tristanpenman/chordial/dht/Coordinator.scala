package com.tristanpenman.chordial.dht

import java.net.InetSocketAddress

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import akka.io.{IO, Udp}
import akka.util.Timeout
import com.tristanpenman.chordial.core.Node

import scala.concurrent.ExecutionContextExecutor
import scala.concurrent.duration._
import scala.util.Random

class Coordinator(keyspaceBits: Int, nodeAddress: String, nodePort: Int, seedNode: Option[SeedNode])
    extends Actor
    with ActorLogging {
  import context.system

  require(keyspaceBits > 0, "keyspaceBits must be a positive Int value")

  private val idModulus = 1 << keyspaceBits

  implicit val ec: ExecutionContextExecutor = context.system.dispatcher

  IO(Udp) ! Udp.Bind(self, new InetSocketAddress(nodeAddress, nodePort))

  // How long Node should wait until an algorithm is considered to have timed out. This should be significantly
  // longer than the external request timeout, as some algorithms will make multiple external requests before
  // running to completion
  private val algorithmTimeout = Timeout(5000.milliseconds)

  // How long to wait when making requests that may be routed to other nodes
  private val externalRequestTimeout = Timeout(500.milliseconds)

  // TODO: Research how to handle collisions...
  val firstNodeId: Long = Random.nextLong(idModulus)
  val firstNode: ActorRef = system.actorOf(
    Node.props(firstNodeId, keyspaceBits, algorithmTimeout, externalRequestTimeout, system.eventStream),
    "node:" + firstNodeId
  )

  seedNode match {
    case Some(value) =>
      log.info(s"seed node: ${value}")
    case _ =>
      log.info("not using a seed node")
  }

  def receive: Receive = {
    case Udp.Bound(_) =>
      context.become(ready(sender()))
  }

  def ready(socket: ActorRef): Receive = {
    // case Udp.Received(data, remote) =>
    case Udp.Unbind  => socket ! Udp.Unbind
    case Udp.Unbound => context.stop(self)
  }
}

object Coordinator {
  def props(keyspaceBits: Int, nodeAddress: String, nodePort: Int, seedNode: Option[SeedNode]): Props =
    Props(new Coordinator(keyspaceBits, nodeAddress, nodePort, seedNode))
}
