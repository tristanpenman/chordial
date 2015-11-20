package com.tristanpenman.chordial.daemon.service

import akka.actor._
import com.tristanpenman.chordial.core.Event
import com.tristanpenman.chordial.core.Event.{PredecessorUpdated, PredecessorReset, SuccessorUpdated}
import spray.can.websocket
import spray.can.websocket.FrameCommandFailed
import spray.can.websocket.frame.{TextFrame, BinaryFrame}
import spray.http.HttpRequest
import spray.routing.HttpServiceActor

class WebSocketWorker(val serverConnection: ActorRef, val nodeRef: ActorRef)
  extends HttpServiceActor with websocket.WebSocketServerWorker with Service {

  private def routesWithEventStream = routes ~ pathPrefix("eventstream") {
    getFromResourceDirectory("webapp")
  }

  private def businessLogicNoUpgrade: Receive = {
    implicit val refFactory: ActorRefFactory = context
    runRoute(routesWithEventStream)
  }

  override def businessLogic: Receive = {
    case x@(_: BinaryFrame | _: TextFrame) => // Bounce back
      sender() ! x

    case e: Event =>
      log.info("Received event")
      e match {
        case PredecessorReset(nodeId) =>
          send(TextFrame(s"Predecessor of node $nodeId has been reset"))
        case PredecessorUpdated(nodeId, predecessorId) =>
          send(TextFrame(s"Predecessor of node $nodeId updated to $predecessorId"))
        case SuccessorUpdated(nodeId, successorId) =>
          send(TextFrame(s"Successor of node $nodeId updated to $successorId"))
      }

    case x: FrameCommandFailed =>
      log.error("frame command failed", x)

    case x: HttpRequest => // do something
  }

  override def receive: Receive = handshaking orElse businessLogicNoUpgrade orElse closeLogic
}

object WebSocketWorker {
  def props(serverConnection: ActorRef, nodeRef: ActorRef): Props =
    Props(new WebSocketWorker(serverConnection, nodeRef))
}
