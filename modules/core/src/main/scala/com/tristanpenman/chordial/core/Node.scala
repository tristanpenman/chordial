package com.tristanpenman.chordial.core

import akka.actor._
import com.tristanpenman.chordial.core.shared.{Interval, NodeInfo}

object NodeProtocol {

  case class ClosestPrecedingFinger(queryId: Long)

  class ClosestPrecedingFingerResponse

  case class ClosestPrecedingFingerOk(queryId: Long, nodeId: Long, nodeRef: ActorRef)
    extends ClosestPrecedingFingerResponse

  case class ClosestPrecedingFingerError(queryId: Long, message: String) extends ClosestPrecedingFingerResponse

  case class GetId()

  class GetIdResponse

  case class GetIdOk(id: Long) extends GetIdResponse

  class GetPredecessorResponse

  case class GetPredecessor()

  case class GetPredecessorOk(predecessorId: Long, predecessorRef: ActorRef) extends GetPredecessorResponse

  case class GetPredecessorOkButUnknown() extends GetPredecessorResponse

  class GetSuccessorResponse

  case class GetSuccessor()

  case class GetSuccessorOk(successorId: Long, successorRef: ActorRef) extends GetSuccessorResponse

  case class UpdatePredecessor(predecessorId: Long, predecessorRef: ActorRef)

  class UpdatePredecessorResponse

  case class UpdatePredecessorOk() extends UpdatePredecessorResponse

  case class UpdateSuccessor(successorId: Long, successorRef: ActorRef)

  class UpdateSuccessorResponse

  case class UpdateSuccessorOk() extends UpdateSuccessorResponse

}

class Node(ownId: Long, seed: NodeInfo) extends Actor with ActorLogging {

  import NodeProtocol._

  //noinspection ScalaStyle
  private def receiveWhileReady(successor: NodeInfo, predecessor: Option[NodeInfo]): Receive = {
    case ClosestPrecedingFinger(queryId) =>
      // Simplified version of the closest-preceding-finger algorithm that does not use a finger table. We first check
      // whether the closest known successor lies in the interval beginning immediately after the current node and
      // ending immediately before the query ID - this corresponds to the case where the successor node is the current
      // node's closest known predecessor for the query ID. Otherwise, the current node is the closest predecessor.
      if (Interval(ownId + 1, queryId).contains(successor.id)) {
        sender() ! ClosestPrecedingFingerOk(queryId, successor.id, successor.ref)
      } else {
        sender() ! ClosestPrecedingFingerOk(queryId, ownId, self)
      }

    case GetId() =>
      sender() ! GetIdOk(ownId)

    case GetPredecessor() =>
      predecessor match {
        case Some(info) =>
          sender() ! GetPredecessorOk(info.id, info.ref)
        case None =>
          sender() ! GetPredecessorOkButUnknown()
      }

    case GetSuccessor() =>
      sender() ! GetSuccessorOk(successor.id, successor.ref)

    case UpdatePredecessor(predecessorId, predecessorRef) =>
      context.become(receiveWhileReady(successor, Some(NodeInfo(predecessorId, predecessorRef))))
      sender() ! UpdatePredecessorOk()

    case UpdateSuccessor(successorId, successorRef) =>
      context.become(receiveWhileReady(NodeInfo(successorId, successorRef), predecessor))
      sender() ! UpdateSuccessorOk()
  }

  override def receive: Receive = receiveWhileReady(seed, None)
}

object Node {
  def props(ownId: Long, seed: NodeInfo): Props =
    Props(new Node(ownId, seed))
}
