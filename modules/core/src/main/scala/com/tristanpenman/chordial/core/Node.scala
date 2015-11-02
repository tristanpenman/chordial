package com.tristanpenman.chordial.core

import akka.actor._
import akka.pattern.ask
import akka.pattern.pipe

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

object NodeProtocol {

  class GetPredecessorResponse

  case class GetPredecessor()

  case class GetPredecessorOk(predecessorId: Long, predecessorRef: ActorRef) extends GetPredecessorResponse

  case class GetPredecessorOkButUnknown() extends GetPredecessorResponse

  case class GetSuccessor()

  case class GetSuccessorOk(successorId: Long, successorRef: ActorRef)

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

  private case class BeginStabilisation()

  private case class StabilisationFailed()

  private case class UpdateSuccessor(successorId: Long, successorRef: ActorRef)

  /** Time to wait between stabilisation attempts */
  private val stabilisationInterval = Duration(2000, MILLISECONDS)

  private val stabilisationTimeout = Duration(5000, MILLISECONDS)

  /** Schedule periodic stabilisation */
  context.system.scheduler.schedule(stabilisationInterval, stabilisationInterval, self, BeginStabilisation())

  private def stabilise(successor: NodeInfo) = {
    successor.ref.ask(GetPredecessor())(stabilisationTimeout)
      .mapTo[GetPredecessorResponse]
      .map({
        case GetPredecessorOk(predecessorId: Long, predecessorRef: ActorRef) =>
          UpdateSuccessor(successor.id, successor.ref)
        case GetPredecessorOkButUnknown() =>
          UpdateSuccessor(successor.id, successor.ref)
      })
      .recover({ case _ => StabilisationFailed() })
      .pipeTo(self)
  }

  private def receiveWhileReady(successor: NodeInfo, predecessor: Option[NodeInfo], stabilising: Boolean): Receive = {
    case BeginStabilisation() =>
      if (!stabilising) {
        context.become(receiveWhileReady(successor, predecessor, stabilising = true))
        stabilise(successor)
      }

    case GetPredecessor =>
      predecessor match {
        case Some(info) =>
          sender() ! GetPredecessorOk(info.id, info.ref)
        case None =>
          sender() ! GetPredecessorOkButUnknown()
      }

    case GetSuccessor =>
      sender() ! GetSuccessorOk(successor.id, successor.ref)

    case Join(seed) =>
      sender() ! JoinError("Not implemented")

    case UpdateSuccessor(successorId: Long, successorRef: ActorRef) =>
      context.become(receiveWhileReady(NodeInfo(successorId, successorRef), predecessor, stabilising = false))
  }

  override def receive = receiveWhileReady(NodeInfo(ownId, self), None, stabilising = false)
}
