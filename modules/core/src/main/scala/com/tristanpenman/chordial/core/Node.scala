package com.tristanpenman.chordial.core

import akka.actor._
import com.tristanpenman.chordial.core.shared.{Interval, NodeInfo}

class Node(ownId: Long, seed: NodeInfo) extends Actor with ActorLogging {

  import Node._

  //noinspection ScalaStyle
  private def receiveWhileReady(successor: NodeInfo, predecessor: Option[NodeInfo]): Receive = {
    case ClosestPrecedingFinger(queryId) =>
      // Simplified version of the closest-preceding-finger algorithm that does not use a finger table. We first check
      // whether the closest known successor lies in the interval beginning immediately after the current node and
      // ending immediately before the query ID - this corresponds to the case where the successor node is the current
      // node's closest known predecessor for the query ID. Otherwise, the current node is the closest predecessor.
      if (Interval(ownId + 1, queryId).contains(successor.id)) {
        sender() ! ClosestPrecedingFingerOk(successor.id, successor.ref)
      } else {
        sender() ! ClosestPrecedingFingerOk(ownId, self)
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

    case ResetPredecessor() =>
      context.become(receiveWhileReady(successor, None))
      sender() ! ResetPredecessorOk()

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

  sealed trait Request

  sealed trait Response

  case class ClosestPrecedingFinger(queryId: Long) extends Request

  sealed trait ClosestPrecedingFingerResponse extends Response

  case class ClosestPrecedingFingerOk(nodeId: Long, nodeRef: ActorRef) extends ClosestPrecedingFingerResponse

  case class ClosestPrecedingFingerError(message: String) extends ClosestPrecedingFingerResponse

  case class GetId() extends Request

  sealed trait GetIdResponse extends Response

  case class GetIdOk(id: Long) extends GetIdResponse

  case class GetPredecessor() extends Request

  sealed trait GetPredecessorResponse extends Response

  case class GetPredecessorOk(predecessorId: Long, predecessorRef: ActorRef) extends GetPredecessorResponse

  case class GetPredecessorOkButUnknown() extends GetPredecessorResponse

  sealed trait GetSuccessorResponse extends Response

  case class GetSuccessor() extends Request

  case class GetSuccessorOk(successorId: Long, successorRef: ActorRef) extends GetSuccessorResponse

  case class ResetPredecessor() extends Request

  sealed trait ResetPredecessorResponse extends Response

  case class ResetPredecessorOk() extends ResetPredecessorResponse

  case class UpdatePredecessor(predecessorId: Long, predecessorRef: ActorRef) extends Request

  sealed trait UpdatePredecessorResponse extends Response

  case class UpdatePredecessorOk() extends UpdatePredecessorResponse

  case class UpdateSuccessor(successorId: Long, successorRef: ActorRef) extends Request

  sealed trait UpdateSuccessorResponse extends Response

  case class UpdateSuccessorOk() extends UpdateSuccessorResponse

  def props(ownId: Long, seed: NodeInfo): Props = Props(new Node(ownId, seed))
}
