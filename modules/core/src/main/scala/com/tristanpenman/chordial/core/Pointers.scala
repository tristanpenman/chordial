package com.tristanpenman.chordial.core

import akka.actor._
import akka.event.EventStream
import com.tristanpenman.chordial.core.Event._
import com.tristanpenman.chordial.core.shared.NodeInfo

final class Pointers(nodeId: Long, fingerTableSize: Int, seedNode: NodeInfo, eventStream: EventStream)
    extends Actor
    with ActorLogging {

  import Pointers._

  private def newFingerTable = Vector.fill(fingerTableSize) {
    None
  }

  private def receiveWhileReady(primarySuccessor: NodeInfo,
                                backupSuccessors: List[NodeInfo],
                                predecessor: Option[NodeInfo],
                                fingerTable: Vector[Option[NodeInfo]]): Receive = {
    case GetId =>
      sender() ! GetIdOk(nodeId)

    case GetPredecessor =>
      predecessor match {
        case Some(info) =>
          sender() ! GetPredecessorOk(info)
        case None =>
          sender() ! GetPredecessorOkButUnknown
      }

    case GetSuccessorList =>
      sender() ! GetSuccessorListOk(primarySuccessor, backupSuccessors)

    case ResetFinger(index: Int) =>
      if (index < 0 || index >= fingerTableSize) {
        sender() ! ResetFingerInvalidIndex
      } else {
        context.become(
          receiveWhileReady(primarySuccessor, backupSuccessors, predecessor, fingerTable.updated(index, None))
        )
        sender() ! ResetFingerOk
        eventStream.publish(FingerReset(nodeId, index))
      }

    case ResetPredecessor =>
      context.become(receiveWhileReady(primarySuccessor, backupSuccessors, None, fingerTable))
      sender() ! ResetPredecessorOk
      eventStream.publish(PredecessorReset(nodeId))

    case UpdateFinger(index: Int, finger: NodeInfo) =>
      if (index < 0 || index >= fingerTableSize) {
        sender() ! UpdateFingerInvalidIndex
      } else {
        context.become(
          receiveWhileReady(primarySuccessor, backupSuccessors, predecessor, fingerTable.updated(index, Some(finger)))
        )
        sender() ! UpdateFingerOk
        eventStream.publish(FingerUpdated(nodeId, index, finger.id))
      }

    case UpdatePredecessor(newPredecessor) =>
      context.become(receiveWhileReady(primarySuccessor, backupSuccessors, Some(newPredecessor), fingerTable))
      sender() ! UpdatePredecessorOk
      eventStream.publish(PredecessorUpdated(nodeId, newPredecessor.id))

    case UpdateSuccessorList(newPrimarySuccessor, newBackupSuccessors) =>
      if (newPrimarySuccessor != primarySuccessor || newBackupSuccessors != backupSuccessors) {
        context.become(receiveWhileReady(newPrimarySuccessor, newBackupSuccessors, predecessor, fingerTable))
        sender() ! UpdateSuccessorListOk
        eventStream.publish(SuccessorListUpdated(nodeId, newPrimarySuccessor.id, newBackupSuccessors.map(_.id)))
      } else {
        sender() ! UpdateSuccessorListOk
      }
  }

  eventStream.publish(NodeCreated(nodeId, seedNode.id))

  override def receive: Receive =
    receiveWhileReady(seedNode, List.empty, None, newFingerTable)
}

object Pointers {

  sealed trait Request

  sealed trait Response

  case object GetId extends Request

  sealed trait GetIdResponse extends Response

  final case class GetIdOk(id: Long) extends GetIdResponse

  case object GetPredecessor extends Request

  sealed trait GetPredecessorResponse extends Response

  final case class GetPredecessorOk(predecessor: NodeInfo) extends GetPredecessorResponse

  case object GetPredecessorOkButUnknown extends GetPredecessorResponse

  sealed trait GetSuccessorListResponse extends Response

  case object GetSuccessorList extends Request

  final case class GetSuccessorListOk(primarySuccessor: NodeInfo, backupSuccessors: List[NodeInfo])
      extends GetSuccessorListResponse

  case object ResetPredecessor extends Request

  sealed trait ResetPredecessorResponse extends Response

  case object ResetPredecessorOk extends ResetPredecessorResponse

  final case class ResetFinger(index: Int) extends Request

  sealed trait ResetFingerResponse extends Response

  case object ResetFingerOk extends ResetFingerResponse

  case object ResetFingerInvalidIndex extends ResetFingerResponse

  final case class UpdateFinger(index: Int, finger: NodeInfo) extends Request

  sealed trait UpdateFingerResponse extends Response

  case object UpdateFingerOk extends UpdateFingerResponse

  case object UpdateFingerInvalidIndex extends UpdateFingerResponse

  final case class UpdatePredecessor(predecessor: NodeInfo) extends Request

  sealed trait UpdatePredecessorResponse extends Response

  case object UpdatePredecessorOk extends UpdatePredecessorResponse

  final case class UpdateSuccessorList(primarySuccessor: NodeInfo, backupSuccessors: List[NodeInfo]) extends Request

  sealed trait UpdateSuccessorListResponse extends Response

  case object UpdateSuccessorListOk extends UpdateSuccessorListResponse

  def props(ownId: Long, keyspaceBits: Int, seed: NodeInfo, eventStream: EventStream): Props =
    Props(new Pointers(ownId, keyspaceBits, seed, eventStream))
}
