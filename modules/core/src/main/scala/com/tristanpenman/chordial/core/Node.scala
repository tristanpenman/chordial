package com.tristanpenman.chordial.core

import akka.actor._

object NodeProtocol {
  case class GetPredecessor()
  case class GetPredecessorOk(predecessorId: Long, predecessorRef: ActorRef)
  case class GetPredecessorOkButUnknown()

  case class GetSuccessor()
  case class GetSuccessorOk(successorId: Long, successorRef: ActorRef)

  case class Hello()
  case class HelloResponse()

  case class Join(seed: Option[ActorRef])
  case class JoinOk()
  case class JoinError(message: String)
}

class Node(ownId: Long) extends Actor with ActorLogging {
  import NodeProtocol._

  private class NodeInfo(nodeId: Long, nodeRef: ActorRef) {
    val id = nodeId
    val ref = nodeRef
  }

  private object NodeInfo {
    def apply(nodeId: Long, nodeRef: ActorRef) = new NodeInfo(nodeId, nodeRef)
  }

  private def receiveWhileReady(successor: NodeInfo, predecessor: Option[NodeInfo]): Receive = {
    case GetPredecessor =>
      predecessor match {
        case Some(info) =>
          sender() ! GetPredecessorOk(info.id, info.ref)
        case None =>
          sender() ! GetPredecessorOkButUnknown()
      }

    case GetSuccessor =>
      sender() ! GetSuccessorOk(successor.id, successor.ref)

    case Hello =>
      sender() ! HelloResponse

    case Join(seed) =>
      sender() ! JoinError("Not implemented")
  }

  override def receive = receiveWhileReady(NodeInfo(ownId, self), None)
}
