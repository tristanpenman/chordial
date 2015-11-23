package com.tristanpenman.chordial.core

import akka.actor._
import akka.event.EventStream
import com.tristanpenman.chordial.core.Event.{NodeCreated, PredecessorReset, PredecessorUpdated, SuccessorUpdated}
import com.tristanpenman.chordial.core.shared.NodeInfo

class Node(ownId: Long, seed: NodeInfo, eventStream: EventStream) extends Actor with ActorLogging {

  import Node._

  private def receiveWhileReady(successor: NodeInfo, predecessor: Option[NodeInfo]): Receive = {
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
      eventStream.publish(PredecessorReset(ownId))

    case UpdatePredecessor(predecessorId, predecessorRef) =>
      context.become(receiveWhileReady(successor, Some(NodeInfo(predecessorId, predecessorRef))))
      sender() ! UpdatePredecessorOk()
      eventStream.publish(PredecessorUpdated(ownId, predecessorId))

    case UpdateSuccessor(successorId, successorRef) =>
      context.become(receiveWhileReady(NodeInfo(successorId, successorRef), predecessor))
      sender() ! UpdateSuccessorOk()
      eventStream.publish(SuccessorUpdated(ownId, successorId))
  }

  eventStream.publish(NodeCreated(ownId, seed.id))

  override def receive: Receive = receiveWhileReady(seed, None)
}

object Node {

  sealed trait Request

  sealed trait Response

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

  def props(ownId: Long, seed: NodeInfo, eventStream: EventStream): Props = Props(new Node(ownId, seed, eventStream))
}
