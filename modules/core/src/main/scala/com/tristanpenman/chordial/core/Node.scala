package com.tristanpenman.chordial.core

import akka.actor._
import akka.pattern.{AskTimeoutException, ask, pipe}
import akka.util.Timeout
import com.tristanpenman.chordial.core.actors.FindPredecessorAlgorithm._
import com.tristanpenman.chordial.core.actors.FindSuccessorAlgorithm._
import com.tristanpenman.chordial.core.actors.StabilisationAlgorithm._
import com.tristanpenman.chordial.core.actors._
import com.tristanpenman.chordial.core.shared.Interval
import com.tristanpenman.chordial.core.shared.NodeInfo

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

object NodeProtocol {

  class ClosestPrecedingFingerResponse

  case class ClosestPrecedingFinger(queryId: Long)

  case class ClosestPrecedingFingerOk(queryId: Long, nodeId: Long, nodeRef: ActorRef)
    extends ClosestPrecedingFingerResponse

  case class ClosestPrecedingFingerError(queryId: Long, message: String) extends ClosestPrecedingFingerResponse

  class FindPredecessorResponse

  case class FindPredecessor(queryId: Long)

  case class FindPredecessorOk(queryId: Long, predecessorId: Long, predecessorRef: ActorRef)
    extends FindPredecessorResponse

  case class FindPredecessorError(queryId: Long, message: String) extends FindPredecessorResponse

  class FindSuccessorResponse

  case class FindSuccessor(queryId: Long)

  case class FindSuccessorOk(queryId: Long, successorId: Long, successorRef: ActorRef)
    extends FindSuccessorResponse

  case class FindSuccessorError(queryId: Long, message: String) extends FindSuccessorResponse

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

  case class JoinOk(successorId: Long) extends JoinResponse

  case class JoinError(message: String) extends JoinResponse

  case class Notify(nodeId: Long, nodeRef: ActorRef)

  class NotifyResponse

  case class NotifyOk() extends NotifyResponse

  case class NotifyIgnored() extends NotifyResponse

  case class Stabilise(sequenceId: Long)

  case class StabiliseInProgress(sequenceId: Long)

  case class StabiliseOk(sequenceId: Long, successorId: Long, successorRef: ActorRef)

  case class StabiliseError(sequenceId: Long, message: String)

  case class UpdateSuccessor(successorId: Long, successorRef: ActorRef)

  class UpdateSuccessorResponse

  case class UpdateSuccessorOk() extends UpdateSuccessorResponse

  class PublishedEvent

  case class JoinedNetwork(ownId: Long, seedId: Long) extends PublishedEvent

  case class PredecessorInitialised(ownId: Long, predecessorId: Long) extends PublishedEvent

  case class PredecessorUpdated(ownId: Long, predecessorId: Long, prevPredecessorId: Option[Long]) extends PublishedEvent

  case class StabilisationStarted(ownId: Long) extends PublishedEvent

  case class StabilisationFinished(ownId: Long, successorId: Long, prevSuccessorId: Long) extends PublishedEvent

  case class StabilisationFinishedWithError(ownId: Long, message: String) extends PublishedEvent

  case class SuccessorNotified(ownId: Long, successorId: Long) extends PublishedEvent

}

class Node(ownId: Long, eventSinks: Set[ActorRef]) extends Actor with ActorLogging {

  import NodeProtocol._

  /** Time to wait for a GetPredecessor response during stabilisation */
  private val stabilisationTimeout = Timeout(5000.milliseconds)

  private val joinTimeout = Timeout(5000.milliseconds)

  private val findPredecessorTimeout = Timeout(5000.milliseconds)

  private val findSuccessorTimeout = Timeout(5000.milliseconds)

  /**
   * Returns true if the current predecessor should be replaced with the candidate node
   */
  private def shouldUpdatePredecessor(currentPred: Option[NodeInfo], candidateId: Long, candidateRef: ActorRef) = {
    currentPred match {
      case Some(pred) => Interval(pred.id + 1, ownId).contains(candidateId)
      case None => true
    }
  }

  /**
   * Create an actor to execute the FindPredecessor algorithm and, when it finishes/fails, produce a response that
   * will be recognised by the original sender of the request
   *
   * This method passes in the ID and ActorRef of the current node as the initial candidate node, which means the
   * FindPredecessor algorithm will begin its search at the current node.
   */
  private def findPredecessor(queryId: Long, sender: ActorRef, requestTimeout: Timeout): Unit = {
    // The FindPredecessorAlgorithm actor will shutdown immediately after it sends a FindPredecessorAlgorithmOk or
    // FindPredecessorAlgorithmError message. However, if the future returned by the 'ask' request does not complete
    // within the timeout period, the actor must be shutdown manually to ensure that it does not run indefinitely.
    val findPredecessorAlgorithm = context.actorOf(FindPredecessorAlgorithm.props())
    findPredecessorAlgorithm.ask(FindPredecessorAlgorithmBegin(queryId, ownId, self))(requestTimeout)
      .mapTo[FindPredecessorAlgorithmResponse]
      .map {
        case FindPredecessorAlgorithmOk(predecessorId, predecessorRef) =>
          FindPredecessorOk(queryId, predecessorId, predecessorRef)
        case FindPredecessorAlgorithmError(message) =>
          FindPredecessorError(queryId, message)
      }
      .recover {
        case exception =>
          context.stop(findPredecessorAlgorithm)
          FindPredecessorError(queryId, exception.getMessage)
      }
      .pipeTo(sender)
  }

  /**
   * Create an actor to execute the FindSuccessor algorithm and, when it finishes/fails, produce a response that
   * will be recognised by the original sender of the request.
   *
   * This method passes in the ActorRef of the current node as the search node, which means the operation will be
   * performed in the context of the current node.
   */
  private def findSuccessor(queryId: Long, sender: ActorRef, requestTimeout: Timeout): Unit = {
    // The FindSuccessorAlgorithm actor will shutdown immediately after it sends a FindSuccessorAlgorithmOk or
    // FindSuccessorAlgorithmError message. However, if the future returned by the 'ask' request does not complete
    // within the timeout period, the actor must be shutdown manually to ensure that it does not run indefinitely.
    val findSuccessorAlgorithm = context.actorOf(FindSuccessorAlgorithm.props())
    findSuccessorAlgorithm.ask(FindSuccessorAlgorithmBegin(queryId, self))(requestTimeout)
      .mapTo[FindSuccessorAlgorithmResponse]
      .map {
        case FindSuccessorAlgorithmOk(successorId, successorRef) =>
          FindSuccessorOk(queryId, successorId, successorRef)
        case FindSuccessorAlgorithmError(message) =>
          FindSuccessorError(queryId, message)
      }
      .recover {
        case exception =>
          context.stop(findSuccessorAlgorithm)
          FindSuccessorError(queryId, exception.getMessage)
      }
      .pipeTo(sender)
  }

  /**
   * Attempt to join an existing Chord network, blocking until the join is successful or the request timeout period has
   * elapsed
   *
   * This is a simplified version of the join algorithm that simply uses the seed node as the successor, which means
   * that the only operation we're blocking on is the request for that node's ID. Although we may already have that
   * information stored locally, this ensures that the node is live.
   */
  private def join(seedRef: ActorRef, sender: ActorRef, requestTimeout: Timeout): Option[NodeInfo] =
    Await.result(seedRef.ask(GetId())(requestTimeout)
      .mapTo[GetIdOk]
      .map {
        case GetIdOk(seedId) =>
          sender ! JoinOk(seedId)
          Some(NodeInfo(seedId, seedRef))
      }
      .recover {
        case exception =>
          sender ! JoinError(exception.getMessage)
          None
      },
      Duration.Inf)

  private def receiveWhileReady(successor: NodeInfo, predecessor: Option[NodeInfo], stabilising: Boolean): Receive = {
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

    case FindPredecessor(queryId) =>
      findPredecessor(queryId, sender(), findPredecessorTimeout)

    case FindSuccessor(queryId) =>
      findSuccessor(queryId, sender(), findSuccessorTimeout)

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
      join(seedRef, sender(), joinTimeout).foreach {
        case newSuccessor =>
          context.become(receiveWhileReady(newSuccessor, None, stabilising = false))
          eventSinks.foreach(_ ! JoinedNetwork(ownId, newSuccessor.id))
      }

    case Notify(candidateId, candidateRef) =>
      if (shouldUpdatePredecessor(predecessor, candidateId, candidateRef)) {
        context.become(receiveWhileReady(successor, Some(NodeInfo(candidateId, candidateRef)), stabilising))
        eventSinks.foreach(_ ! PredecessorUpdated(ownId, candidateId, predecessor.map(_.id)))
        sender() ! NotifyOk()
      } else {
        sender() ! NotifyIgnored()
      }

    case Stabilise(sequenceId) if stabilising =>
      sender() ! StabiliseInProgress(sequenceId)

    case Stabilise(sequenceId) =>
      val replyTo = sender()
      val stabilisationAlgorithm = context.actorOf(StabilisationAlgorithm.props())
      try {
        context.become(receiveWhileReady(successor, predecessor, stabilising = true))
        stabilisationAlgorithm.ask(StabilisationAlgorithmStart(NodeInfo(ownId, self)))(stabilisationTimeout)
          .onComplete { result =>
            context.stop(stabilisationAlgorithm)
            context.become(receiveWhileReady(successor, predecessor, stabilising = false))
            result match {
              case util.Success(StabilisationAlgorithmFinished(newSuccessor)) =>
                replyTo ! StabiliseOk(sequenceId, newSuccessor.id, newSuccessor.ref)
              case util.Success(unexpected) =>
                replyTo ! StabiliseError(sequenceId, "Stabilisation failed due internal error")
              case util.Failure(exception: AskTimeoutException) =>
                replyTo ! StabiliseError(sequenceId, "Stabilisation algorithm timed out")
              case util.Failure(exception) =>
                replyTo ! StabiliseError(sequenceId, s"Stabilisation algorithm failed: ${exception.getMessage}")
            }
          }
      } catch {
        case _: Throwable =>
          context.stop(stabilisationAlgorithm)
          context.become(receiveWhileReady(successor, predecessor, stabilising = false))
          replyTo ! StabiliseError(sequenceId, "Stabilisation algorithm could not be started")
      }


    case UpdateSuccessor(successorId, successorRef) =>
      context.become(receiveWhileReady(NodeInfo(successorId, successorRef), predecessor, stabilising))
      successorRef ! Notify(ownId, self)
      eventSinks.foreach(_ ! SuccessorNotified(ownId, successorId))
      sender() ! UpdateSuccessorOk()
  }

  override def receive: Receive = receiveWhileReady(NodeInfo(ownId, self), None, stabilising = false)
}

object Node {
  def props(ownId: Long, eventSinks: Set[ActorRef] = Set.empty): Props = Props(new Node(ownId, eventSinks))
}
