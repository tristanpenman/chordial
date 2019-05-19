package com.tristanpenman.chordial.demo

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.ws.TextMessage
import akka.stream.scaladsl._
import akka.stream.{ActorAttributes, ActorMaterializer, OverflowStrategy, Supervision}
import akka.util.Timeout
import com.tristanpenman.chordial.core.Event
import com.tristanpenman.chordial.core.Event._

import scala.concurrent.Await
import scala.concurrent.duration._

object Demo extends App {
  implicit val system = ActorSystem("chordial-demo")
  implicit val mat = ActorMaterializer()
  implicit val ec = system.dispatcher

  implicit val timeout: Timeout = 3.seconds

  // Generate IDs ranging from 0 to 63 (inclusive) so that when visualising the network,
  // each node represents a ~5.625 degree arc on the ring
  private val keyspaceBits = 6

  // Create an actor that is responsible for creating and terminating nodes, while ensuring
  // that nodes are assigned unique IDs in the Chord key-space
  private val governor =
    system.actorOf(Governor.props(keyspaceBits), "Governor")

  // Create an actor that will log events published by nodes
  private val eventWriter = system.actorOf(EventWriter.props, "EventWriter")

  // Subscribe the EventWriter actor to events published by nodes
  system.eventStream.subscribe(eventWriter, classOf[Event])

  val (listener, eventsSource) =
    Source
      .actorRef[Event](Int.MaxValue, OverflowStrategy.fail)
      .map {
        case FingerReset(nodeId: Long, index: Int) =>
          s"""{ "type": "FingerReset", "nodeId": $nodeId, "index": $index }"""
        case FingerUpdated(nodeId: Long, index: Int, fingerId: Long) =>
          s"""{ "type": "FingerUpdated", "nodeId": $nodeId, "index": $index, "fingerId": $fingerId }"""
        case NodeCreated(nodeId, successorId) =>
          s"""{ "type": "NodeCreated", "nodeId": $nodeId, "successorId": $successorId }"""
        case NodeShuttingDown(nodeId) =>
          s"""{ "type": "NodeDeleted", "nodeId": $nodeId }"""
        case PredecessorReset(nodeId) =>
          s"""{ "type": "PredecessorReset", "nodeId": $nodeId }"""
        case PredecessorUpdated(nodeId, predecessorId) =>
          s"""{ "type": "PredecessorUpdated", "nodeId": $nodeId, "predecessorId": $predecessorId }"""
        case SuccessorListUpdated(nodeId, primarySuccessorId, _) =>
          s"""{ "type": "SuccessorUpdated", "nodeId": $nodeId, "successorId": $primarySuccessorId }"""
      }
      .map(TextMessage(_))
      .withAttributes(ActorAttributes.supervisionStrategy(Supervision.resumingDecider))
      .toMat(BroadcastHub.sink[TextMessage](bufferSize = 16))(Keep.both)
      .run()

  system.eventStream.subscribe(listener, classOf[Event])

  Http().bindAndHandle(WebSocketWorker(governor, eventsSource), "0.0.0.0", 4567)

  Await.result(system.whenTerminated, Duration.Inf)
}
