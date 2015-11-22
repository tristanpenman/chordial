package com.tristanpenman.chordial.daemon

import akka.actor.{Props, Actor, ActorLogging}
import com.tristanpenman.chordial.core.Event.{NodeCreated, PredecessorReset, PredecessorUpdated, SuccessorUpdated}

class EventWriter extends Actor with ActorLogging {

  override def receive: Receive = {
    case NodeCreated(nodeId, successorId) =>
      log.info( s"""Node created: { "nodeId": $nodeId, "successorId": $successorId }""")
    case PredecessorReset(nodeId) =>
      log.info( s"""Predecessor reset: { "nodeId": $nodeId }""")
    case PredecessorUpdated(nodeId, predecessorId) =>
      log.info( s"""Predecessor updated: { "nodeId": $nodeId, "predecessorId": $predecessorId }""")
    case SuccessorUpdated(nodeId, successorId) =>
      log.info( s"""Successor updated: { "nodeId": $nodeId, "successorId": $successorId }""")
  }

}

object EventWriter {
  def props: Props = Props(new EventWriter)
}
