package com.tristanpenman.chordial.daemon

import akka.actor.{ActorRef, ActorSystem}
import akka.io.IO
import akka.pattern.ask
import akka.util.Timeout
import com.tristanpenman.chordial.core.{Event, Coordinator}
import com.tristanpenman.chordial.core.Coordinator._
import com.tristanpenman.chordial.daemon.service.WebSocketServer
import spray.can.Http
import spray.can.server.UHttp

import scala.annotation.tailrec
import scala.concurrent.duration._
import scala.io.StdIn

import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.Random

object Daemon extends App {

  implicit val system = ActorSystem("chordial-daemon")
  implicit val timeout = Timeout(5.seconds)

  private val log = system.log
  private val httpPortNumber = 4567
  private val requestTimeout = Timeout(4000.milliseconds)
  private val checkPredecessorTimeout = Timeout(2500.milliseconds)
  private val livenessCheckDuration = 2000.milliseconds
  private val stabiliseTimeout = Timeout(1500.milliseconds)

  // Generate IDs ranging from 0 to 359 (inclusive) so that when visualising the network,
  // each node can be represented as a one degree arc on the ring
  private val idModulus = 360

  private var nodeIds: Set[Long] = Set.empty

  private def scheduleCheckPredecessor(nodeRef: ActorRef) =
    system.scheduler.schedule(3000.milliseconds, 300.milliseconds) {
      nodeRef.ask(CheckPredecessor())(checkPredecessorTimeout)
        .mapTo[CheckPredecessorResponse]
        .onComplete {
          case util.Success(result) => result match {
            case CheckPredecessorOk() =>
              log.debug("CheckPredecessor (requested for {}) finished successfully", nodeRef.path)
            case CheckPredecessorInProgress() =>
              log.warning("CheckPredecessor (requested for {}) already in progress", nodeRef.path)
            case CheckPredecessorError(message) =>
              log.error("CheckPredecessor (requested for {}) finished with error: {}", nodeRef.path, message)
          }
          case util.Failure(exception) =>
            log.error("CheckPredecessor (requested for {}) failed with an exception: {}", nodeRef.path, exception)
        }
    }

  private def scheduleStabilisation(nodeRef: ActorRef) =
    system.scheduler.schedule(10000.milliseconds, 200.milliseconds) {
      nodeRef.ask(Stabilise())(stabiliseTimeout)
        .mapTo[StabiliseResponse]
        .onComplete {
          case util.Success(result) => result match {
            case StabiliseOk() =>
              log.debug("Stabilisation (requested for {}) finished successfully", nodeRef.path)
            case StabiliseInProgress() =>
              log.warning("Stabilisation (requested for {}) already in progress", nodeRef.path)
            case StabiliseError(message) =>
              log.error("Stabilisation (requested for {}) finished with error: {}", nodeRef.path, message)
          }
          case util.Failure(exception) =>
            log.error("Stabilisation (requested for {}) failed with an exception: {}", nodeRef.path, exception)
        }
    }

  private def createNode(nodeId: Long): ActorRef = {
    val nodeRef = system.actorOf(Coordinator.props(nodeId, requestTimeout, livenessCheckDuration, system.eventStream))
    scheduleCheckPredecessor(nodeRef)
    scheduleStabilisation(nodeRef)
    nodeRef
  }

  @tailrec
  private def generateUniqueId(nodeIds: Set[Long]): Long = {
    val id = Random.nextInt(idModulus)
    if (!nodeIds.contains(id)) {
      id
    } else {
      generateUniqueId(nodeIds)
    }
  }

  val seedId = 0L
  nodeIds += seedId
  val seedRef = createNode(seedId)

  private val server = system.actorOf(WebSocketServer.props(seedRef), "websocket")

  IO(UHttp) ? Http.Bind(server, "localhost", httpPortNumber)

  system.eventStream.subscribe(server, classOf[Event])

  for (i <- 1 until 50) {
    val nodeId = generateUniqueId(nodeIds)
    nodeIds += nodeId
    val nodeRef = createNode(nodeId)
    nodeRef ! Join(seedId, seedRef)
    log.info(s"Created node #$i with ID $nodeId")
  }

  StdIn.readLine("Hit ENTER to exit ...\n")
  system.shutdown()
  system.awaitTermination()
}
