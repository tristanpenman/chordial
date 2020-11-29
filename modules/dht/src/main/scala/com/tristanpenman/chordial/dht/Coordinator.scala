package com.tristanpenman.chordial.dht

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import akka.util.Timeout
import com.tristanpenman.chordial.core.Router.{Start, StartFailed, StartOk}
import com.tristanpenman.chordial.core.{Node, Router}

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

  // How long Node should wait until an algorithm is considered to have timed out. This should be significantly
  // longer than the external request timeout, as some algorithms will make multiple external requests before
  // running to completion
  private val algorithmTimeout = Timeout(5000.milliseconds)

  // How long to wait when making requests that may be routed to other nodes
  private val externalRequestTimeout = Timeout(500.milliseconds)

  // Start the router
  private val router = system.actorOf(Router.props())
  router ! Start(nodeAddress, nodePort)

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

  def ready: Receive = {
    case m =>
      log.debug("Received message", m)
  }

  override def receive: Receive = {
    case StartOk() =>
      context.become(ready)

    case StartFailed(reason) =>
      log.error(s"Failed to start Router: ${reason}")
      context.stop(self)

    case m =>
      log.warning("Unexpected message", m)
  }
}

object Coordinator {
  def props(keyspaceBits: Int, nodeAddress: String, nodePort: Int, seedNode: Option[SeedNode]): Props =
    Props(new Coordinator(keyspaceBits, nodeAddress, nodePort, seedNode))
}
