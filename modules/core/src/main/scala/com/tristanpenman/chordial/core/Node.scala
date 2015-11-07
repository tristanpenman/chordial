package com.tristanpenman.chordial.core

import akka.actor._
import akka.pattern.ask
import akka.pattern.pipe
import akka.util.Timeout
import com.tristanpenman.chordial.core.shared.Interval

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

object NodeProtocol {

  class ClosestPrecedingFingerResponse

  case class ClosestPrecedingFinger(queryId: Long)

  case class ClosestPrecedingFingerOk(queryId: Long, nodeId: Long, nodeRef: ActorRef)
    extends ClosestPrecedingFingerResponse

  case class ClosestPrecedingFingerError(queryId: Long, message: String) extends ClosestPrecedingFingerResponse

  class GetIdResponse

  case class GetId()

  case class GetIdOk(id: Long) extends GetIdResponse

  class GetPredecessorResponse

  case class GetPredecessor()

  case class GetPredecessorOk(predecessorId: Long, predecessorRef: ActorRef) extends GetPredecessorResponse

  case class GetPredecessorOkButUnknown() extends GetPredecessorResponse

  class GetSuccessorResponse

  case class GetSuccessor()

  case class GetSuccessorOk(successorId: Long, successorRef: ActorRef) extends GetSuccessorResponse

  class JoinResponse

  case class Join(seed: ActorRef)

  case class JoinOk() extends JoinResponse

  case class JoinError(message: String) extends JoinResponse

  class PublishedEvent

  case class PredecessorInitialised(ownId: Long, predecessorId: Long) extends PublishedEvent

  case class PredecessorUpdated(ownId: Long, predecessorId: Long, prevPredecessorId: Option[Long]) extends PublishedEvent

  case class StabilisationStarted(ownId: Long) extends PublishedEvent

  case class StabilisationFinished(ownId: Long, successorId: Long, prevSuccessorId: Long) extends PublishedEvent

  case class StabilisationFinishedWithError(ownId: Long, message: String) extends PublishedEvent

  case class SuccessorNotified(ownId: Long, successorId: Long) extends PublishedEvent

}

class Node(ownId: Long, eventSinks: Set[ActorRef]) extends Actor with ActorLogging {

  import NodeProtocol._

  private class NodeInfo(nodeId: Long, nodeRef: ActorRef) {
    val id = nodeId
    val ref = nodeRef
  }

  private object NodeInfo {
    def apply(nodeId: Long, nodeRef: ActorRef): NodeInfo = new NodeInfo(nodeId, nodeRef)
  }

  /** Internal signal to trigger stabilisation, fired at a regular interval */
  private case class BeginStabilisation()

  /** Message sent to another node to let it know that it is the closest known successor for the specified node */
  private case class NotifySuccessor(nodeId: Long, nodeRef: ActorRef)

  /** Internal signal to indicate that stabilisation has finished, with details for the closest known successor */
  private case class StabilisationComplete(stabilisationId: Long, successorId: Long, successorRef: ActorRef)

  /** Internal signal to indicate that stabilisation failed */
  private case class StabilisationFailed(stabilisationId: Long, message: String)

  /** Time to wait between stabilisation attempts */
  private val stabilisationInterval = Duration(2000, MILLISECONDS)

  /** Time to wait for a GetPredecessor response during stabilisation */
  private val stabilisationTimeout = Timeout(5000, MILLISECONDS)

  private val joinTimeout = Timeout(5000, MILLISECONDS)

  /** Schedule periodic stabilisation */
  context.system.scheduler.schedule(stabilisationInterval, stabilisationInterval, self, BeginStabilisation())

  /**
   * Send a GetPredecessor request to the current node's closest known successor, and verify that the current node is
   * returned as its predecessor. If another node has joined the network and is located between the current node and
   * its closest known successor, that node should be recorded as the new closest known successor.
   *
   * @param successor NodeInfo for the closest known successor
   */
  private def stabilise(stabilisationId: Long, successor: NodeInfo): Unit = {
    eventSinks.foreach { _ ! StabilisationStarted(ownId) }
    successor.ref.ask(GetPredecessor())(stabilisationTimeout)
      .mapTo[GetPredecessorResponse]
      .map {
        case GetPredecessorOk(predId: Long, predRef: ActorRef) if Interval(ownId + 1, successor.id).contains(predId) =>
          StabilisationComplete(stabilisationId, predId, predRef)
        case GetPredecessorOk(_, _) | GetPredecessorOkButUnknown() =>
          StabilisationComplete(stabilisationId, successor.id, successor.ref)
        case message =>
          StabilisationFailed(stabilisationId, s"Received unexpected ${message.getClass.getSimpleName} message from " +
            "client while waiting for GetPredecessorResponse")
      }
      .recover { case e => StabilisationFailed(stabilisationId, e.getMessage) }
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

  private def receiveWhileReady(successor: NodeInfo, predecessor: Option[NodeInfo],
                                nextStabilisationId: Long, pendingStabilisationId: Option[Long]): Receive = {
    case BeginStabilisation() =>
      if (pendingStabilisationId.isEmpty) {
        context.become(receiveWhileReady(successor, predecessor, nextStabilisationId + 1,
          Some(nextStabilisationId)))
        stabilise(nextStabilisationId, successor)
      }

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

    case Join(seedRef) =>
      try {
        val future = seedRef.ask(GetId())(joinTimeout)
          .mapTo[GetIdOk]
          .map {
            case GetIdOk(id) => id
          }
        val seedId = Await.result(future, Duration.Inf)
        context.become(receiveWhileReady(NodeInfo(seedId, seedRef), None, nextStabilisationId, None))
        sender() ! JoinOk()
      } catch {
        case t: Throwable => sender() ! JoinError(t.getMessage)
      }

    case NotifySuccessor(candidateId: Long, candidateRef: ActorRef) =>
      if (shouldUpdatePredecessor(predecessor, candidateId, candidateRef)) {
        context.become(receiveWhileReady(successor, Some(NodeInfo(candidateId, candidateRef)), nextStabilisationId,
          pendingStabilisationId))
        eventSinks.foreach { _ ! PredecessorUpdated(ownId, candidateId, predecessor.map(_.id)) }
      }

    case StabilisationComplete(stabilisationId: Long, successorId: Long, successorRef: ActorRef) =>
      pendingStabilisationId.foreach(expectedStabilisationId => {
        if (expectedStabilisationId == stabilisationId) {
          context.become(receiveWhileReady(NodeInfo(successorId, successorRef), predecessor, nextStabilisationId, None))
          eventSinks.foreach { _ ! StabilisationFinished(ownId, successorId, successor.id) }
          successorRef ! NotifySuccessor(ownId, self)
          eventSinks.foreach { _ ! SuccessorNotified(ownId, successorId) }
        }
      })

    case StabilisationFailed(stabilisationId: Long, message: String) =>
      pendingStabilisationId.foreach(expectedStabilisationId => {
        if (expectedStabilisationId == stabilisationId) {
          context.become(receiveWhileReady(successor, predecessor, nextStabilisationId, None))
          eventSinks.foreach { _ ! StabilisationFinishedWithError(ownId, message) }
        }
      })
  }

  override def receive: Receive = receiveWhileReady(NodeInfo(ownId, self), None, 0, None)
}

object Node {
    def props(ownId: Long, eventSinks: Set[ActorRef] = Set.empty): Props = Props(new Node(ownId, eventSinks))
}
