package com.tristanpenman.chordial.demo

import akka.actor.{Props, Actor, ActorLogging}
import com.tristanpenman.chordial.core.Event.{NodeCreated, PredecessorReset, PredecessorUpdated, SuccessorListUpdated}

class EventWriter extends Actor with ActorLogging {

  override def receive: Receive = {
    case NodeCreated(nodeId, successorId) =>
      log.info( s"""Node created: { "nodeId": $nodeId, "successorId": $successorId }""")
    case PredecessorReset(nodeId) =>
      log.info( s"""Predecessor reset: { "nodeId": $nodeId }""")
    case PredecessorUpdated(nodeId, predecessorId) =>
      log.info( s"""Predecessor updated: { "nodeId": $nodeId, "predecessorId": $predecessorId }""")
    case SuccessorListUpdated(nodeId, primarySuccessorId, _) =>
      log.info( s"""Successor updated: { "nodeId": $nodeId, "successorId": $primarySuccessorId }""")
  }

}

object EventWriter {
  def props: Props = Props(new EventWriter)
}
