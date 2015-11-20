package com.tristanpenman.chordial.daemon

import akka.actor.{ActorRef, ActorSystem}
import akka.io.IO
import akka.pattern.ask
import akka.util.Timeout
import com.tristanpenman.chordial.core.Coordinator
import com.tristanpenman.chordial.core.Coordinator._
import com.tristanpenman.chordial.daemon.service.WebSocketServer
import spray.can.Http
import spray.can.server.UHttp

import scala.concurrent.duration._
import scala.io.StdIn

import scala.concurrent.ExecutionContext.Implicits.global

object Daemon extends App {

  implicit val system = ActorSystem("chordial-daemon")
  implicit val timeout = Timeout(5.seconds)

  private val log = system.log
  private val httpPortNumber = 4567
  private val requestTimeout = Timeout(4000.milliseconds)
  private val checkPredecessorTimeout = Timeout(2500.milliseconds)
  private val livenessCheckDuration = 2000.milliseconds
  private val stabiliseTimeout = Timeout(1500.milliseconds)

  private def scheduleCheckPredecessor(nodeRef: ActorRef) =
    system.scheduler.schedule(3000.milliseconds, 3000.milliseconds) {
      nodeRef.ask(CheckPredecessor())(checkPredecessorTimeout)
        .mapTo[CheckPredecessorResponse]
        .onComplete {
          case util.Success(result) => result match {
            case CheckPredecessorOk() =>
              log.debug("CheckPredecessor (requested for {}) finished successfully", nodeRef.path)
            case CheckPredecessorInProgress() =>
              log.debug("CheckPredecessor (requested for {}) already in progress", nodeRef.path)
            case CheckPredecessorError(message) =>
              log.error("CheckPredecessor (requested for {}) finished with error: {}", nodeRef.path, message)
          }
          case util.Failure(exception) =>
            log.error("CheckPredecessor (requested for {}) failed with an exception: {}", nodeRef.path, exception)
        }
    }

  private def scheduleStabilisation(nodeRef: ActorRef) =
    system.scheduler.schedule(2000.milliseconds, 2000.milliseconds) {
      nodeRef.ask(Stabilise())(stabiliseTimeout)
        .mapTo[StabiliseResponse]
        .onComplete {
          case util.Success(result) => result match {
            case StabiliseOk() =>
              log.debug("Stabilisation (requested for {}) finished successfully", nodeRef.path)
            case StabiliseInProgress() =>
              log.debug("Stabilisation (requested for {}) already in progress", nodeRef.path)
            case StabiliseError(message) =>
              log.error("Stabilisation (requested for {}) finished with error: {}", nodeRef.path, message)
          }
          case util.Failure(exception) =>
            log.error("Stabilisation (requested for {}) failed with an exception: {}", nodeRef.path, exception)
        }
    }

  private val nodeId = 0L
  private val nodeRef = system.actorOf(Coordinator.props(nodeId, requestTimeout, livenessCheckDuration,
    system.eventStream))

  private val server = system.actorOf(WebSocketServer.props(nodeRef), "websocket")

  IO(UHttp) ? Http.Bind(server, "localhost", httpPortNumber)

  scheduleCheckPredecessor(nodeRef)
  scheduleStabilisation(nodeRef)

  StdIn.readLine("Hit ENTER to exit ...\n")
  system.shutdown()
  system.awaitTermination()
}
