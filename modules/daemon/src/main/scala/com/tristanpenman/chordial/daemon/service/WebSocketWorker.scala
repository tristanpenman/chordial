package com.tristanpenman.chordial.daemon.service

import akka.actor._
import com.tristanpenman.chordial.core.Event
import com.tristanpenman.chordial.core.Event.{PredecessorUpdated, PredecessorReset, SuccessorUpdated}
import spray.can.websocket
import spray.can.websocket.FrameCommandFailed
import spray.can.websocket.frame.{TextFrame, BinaryFrame}
import spray.http.HttpRequest
import spray.routing.HttpServiceActor

class WebSocketWorker(val serverConnection: ActorRef, val governor: ActorRef)
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
      e match {
        case PredecessorReset(nodeId) =>
          send(TextFrame(s"""{ "type": "PredecessorReset", "nodeId": $nodeId }"""))
        case PredecessorUpdated(nodeId, predecessorId) =>
          send(TextFrame(s"""{ "type": "PredecessorUpdated", "nodeId": $nodeId, "predecessorId": $predecessorId }"""))
        case SuccessorUpdated(nodeId, successorId) =>
          send(TextFrame(s"""{ "type": "SuccessorUpdated", "nodeId": $nodeId, "successorId": $successorId }"""))
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
