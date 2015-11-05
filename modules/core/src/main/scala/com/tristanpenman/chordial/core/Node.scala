package com.tristanpenman.chordial.core

import akka.actor._
import akka.pattern.ask
import akka.pattern.pipe
import com.tristanpenman.chordial.core.shared.Interval

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

object NodeProtocol {

  class GetPredecessorResponse

  case class GetPredecessor()

  case class GetPredecessorOk(predecessorId: Long, predecessorRef: ActorRef) extends GetPredecessorResponse

  case class GetPredecessorOkButUnknown() extends GetPredecessorResponse

  class GetSuccessorResponse

  case class GetSuccessor()

  case class GetSuccessorOk(successorId: Long, successorRef: ActorRef) extends GetSuccessorResponse

  class JoinResponse

  case class Join(seed: Option[ActorRef])

  case class JoinOk() extends JoinResponse

  case class JoinError(message: String) extends JoinResponse

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

  /** Internal signal to trigger stabilisation, fired at a regular interval */
  private case class BeginStabilisation()

  /** Message sent to another node to let it know that it is the closest known successor for the specified node */
  private case class NotifySuccessor(nodeId: Long, nodeRef: ActorRef)

  /** Internal signal to indicate that stabilisation has finished, with details for the closest known successor */
  private case class StabilisationComplete(successorId: Long, successorRef: ActorRef)

  /** Internal signal to indicate that stabilisation failed */
  private case class StabilisationFailed()

  /** Time to wait between stabilisation attempts */
  private val stabilisationInterval = Duration(2000, MILLISECONDS)

  /** Time to wait for a GetPredecessor response during stabilisation */
  private val stabilisationTimeout = Timeout(5000, MILLISECONDS)

  /** Schedule periodic stabilisation */
  context.system.scheduler.schedule(stabilisationInterval, stabilisationInterval, self, BeginStabilisation())

  /**
   * Send a GetPredecessor request to the current node's closest known successor, and verify that the current node is
   * returned as its predecessor. If another node has joined the network and is located between the current node and
   * its closest known successor, that node should be recorded as the new closest known successor.
   *
   * @param successor NodeInfo for the closest known successor
   */
  private def stabilise(successor: NodeInfo): Unit = {
    successor.ref.ask(GetPredecessor())(stabilisationTimeout)
      .mapTo[GetPredecessorResponse]
      .map {
        case GetPredecessorOk(predId: Long, predRef: ActorRef) if Interval(ownId + 1, successor.id).contains(predId) =>
          StabilisationComplete(predId, predRef)
        case _ =>
          StabilisationComplete(successor.id, successor.ref)
      }
      .recover { case _ => StabilisationFailed() }
      .pipeTo(self)
    ()
  }

  /**
   * Returns true if the current predecessor should be replaced with the candidate node
   */
  private def shouldUpdatePredecessor(currentPred: Option[NodeInfo], candidateId: Long, candidateRef: ActorRef) = {
    currentPred match {
      case Some(pred) => Interval(pred.id + 1, ownId).contains(candidateId)
      case None => true
    }
  }

  private def receiveWhileReady(successor: NodeInfo, predecessor: Option[NodeInfo], stabilising: Boolean): Receive = {
    case BeginStabilisation() =>
      if (!stabilising) {
        context.become(receiveWhileReady(successor, predecessor, stabilising = true))
        stabilise(successor)
      }

    case GetPredecessor() =>
      predecessor match {
        case Some(info) =>
          sender() ! GetPredecessorOk(info.id, info.ref)
        case None =>
          sender() ! GetPredecessorOkButUnknown()
      }

    case GetSuccessor() =>
      sender() ! GetSuccessorOk(successor.id, successor.ref)

    case Join(seed) =>
      sender() ! JoinError("Not implemented")

    case NotifySuccessor(candidateId: Long, candidateRef: ActorRef) =>
      if (shouldUpdatePredecessor(predecessor, candidateId, candidateRef)) {
        context.become(receiveWhileReady(successor, Some(NodeInfo(candidateId, candidateRef)), stabilising))
      }

    case StabilisationComplete(successorId: Long, successorRef: ActorRef) =>
      context.become(receiveWhileReady(NodeInfo(successorId, successorRef), predecessor, stabilising = false))
      successorRef ! NotifySuccessor(ownId, self)

    case StabilisationFailed() =>
      context.become(receiveWhileReady(successor, predecessor, stabilising = false))
  }

  override def receive = receiveWhileReady(NodeInfo(ownId, self), None, stabilising = false)
}
