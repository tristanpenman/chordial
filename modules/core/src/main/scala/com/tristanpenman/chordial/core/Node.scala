package com.tristanpenman.chordial.core

import akka.actor._
import akka.event.EventStream
import com.tristanpenman.chordial.core.Event._
import com.tristanpenman.chordial.core.shared.NodeInfo

class Node(ownId: Long, keyspaceBits: Int, seed: NodeInfo, eventStream: EventStream) extends Actor with ActorLogging {

  import Node._

  // Check that space is reasonable
  require(keyspaceBits > 0, "keyspaceBits must be a positive Int value")

  private val idModulus = 1 << keyspaceBits

  // Check that node ID is reasonable
  require(ownId >= 0, "ownId must be a non-negative Long value")
  require(ownId < idModulus, s"ownId must be less than $idModulus (2^$keyspaceBits})")

  // Check that seed ID is reasonable
  require(seed.id >= 0, "seed.id must be non-negative Long value")
  require(seed.id < idModulus, s"seed.id must be less than $idModulus (2^$keyspaceBits})")

  private val newFingerTable = Vector.fill(keyspaceBits - 1){None}

  private def receiveWhileReady(successor: NodeInfo, predecessor: Option[NodeInfo],
                                fingerTable: Vector[Option[NodeInfo]]): Receive = {
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

    case ResetFinger(index: Int) =>
      if (index < 1 || index >= keyspaceBits) {
        sender() ! ResetFingerInvalidRequest("Invalid finger table index")
      } else {
        context.become(receiveWhileReady(successor, predecessor, fingerTable.updated(index - 1, None)))
        sender() ! ResetFingerOk()
        eventStream.publish(FingerReset(ownId, index))
      }

    case ResetPredecessor() =>
      context.become(receiveWhileReady(successor, None, fingerTable))
      sender() ! ResetPredecessorOk()
      eventStream.publish(PredecessorReset(ownId))

    case UpdateFinger(index: Int, finger: NodeInfo) =>
      if (index < 0 || index >= keyspaceBits) {
        sender() ! UpdateFingerInvalidRequest("Invalid finger table index")
      } else if (finger.id < 0 || finger.id >= idModulus) {
        sender() ! UpdateFingerInvalidRequest("Invalid finger ID")
      } else {
        if (index == 0) {
          context.become(receiveWhileReady(finger, predecessor, fingerTable))
        } else {
          context.become(receiveWhileReady(successor, predecessor, fingerTable.updated(index - 1, Some(finger))))
        }
        sender() ! UpdateFingerOk()
        eventStream.publish(FingerUpdated(ownId, index, finger.id))
      }

    case UpdatePredecessor(predecessorId, predecessorRef) =>
      if (predecessorId < 0 && predecessorId >= idModulus) {
        sender() ! UpdatePredecessorInvalidRequest("Invalid predecessor ID")
      } else {
        context.become(receiveWhileReady(successor, Some(NodeInfo(predecessorId, predecessorRef)), fingerTable))
        sender() ! UpdatePredecessorOk()
        eventStream.publish(PredecessorUpdated(ownId, predecessorId))
      }

    case UpdateSuccessor(successorId, successorRef) =>
      if (successorId < 0 || successorId >= idModulus) {
        sender() ! UpdateSuccessorInvalidRequest("Invalid successor ID")
      } else {
        context.become(receiveWhileReady(NodeInfo(successorId, successorRef), predecessor, fingerTable))
        sender() ! UpdateSuccessorOk()
        eventStream.publish(SuccessorUpdated(ownId, successorId))
      }
  }

  eventStream.publish(NodeCreated(ownId, seed.id))

  override def receive: Receive = receiveWhileReady(seed, None, newFingerTable)
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

  case class ResetFinger(index: Int) extends Request

  sealed trait ResetFingerResponse extends Response

  case class ResetFingerOk() extends ResetFingerResponse

  case class ResetFingerInvalidRequest(message: String) extends ResetFingerResponse

  case class UpdateFinger(index: Int, finger: NodeInfo) extends Request

  sealed trait UpdateFingerResponse extends Response

  case class UpdateFingerOk() extends UpdateFingerResponse

  case class UpdateFingerInvalidRequest(message: String) extends UpdateFingerResponse

  case class UpdatePredecessor(predecessorId: Long, predecessorRef: ActorRef) extends Request

  sealed trait UpdatePredecessorResponse extends Response

  case class UpdatePredecessorOk() extends UpdatePredecessorResponse

  case class UpdatePredecessorInvalidRequest(message: String) extends UpdatePredecessorResponse

  case class UpdateSuccessor(successorId: Long, successorRef: ActorRef) extends Request

  sealed trait UpdateSuccessorResponse extends Response

  case class UpdateSuccessorOk() extends UpdateSuccessorResponse

  case class UpdateSuccessorInvalidRequest(message: String) extends UpdateSuccessorResponse

  def props(ownId: Long, keyspaceBits: Int, seed: NodeInfo, eventStream: EventStream): Props =
    Props(new Node(ownId, keyspaceBits, seed, eventStream))
}
