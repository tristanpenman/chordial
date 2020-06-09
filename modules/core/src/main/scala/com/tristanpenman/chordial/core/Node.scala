package com.tristanpenman.chordial.core

import akka.actor._
import akka.event.EventStream
import akka.pattern.{ask, pipe}
import akka.util.Timeout
import com.tristanpenman.chordial.core.algorithms.CheckPredecessorAlgorithm._
import com.tristanpenman.chordial.core.algorithms.ClosestPrecedingNodeAlgorithm._
import com.tristanpenman.chordial.core.algorithms.FindPredecessorAlgorithm._
import com.tristanpenman.chordial.core.algorithms.FindSuccessorAlgorithm._
import com.tristanpenman.chordial.core.algorithms.NotifyAlgorithm._
import com.tristanpenman.chordial.core.algorithms.StabilisationAlgorithm._
import com.tristanpenman.chordial.core.algorithms._
import com.tristanpenman.chordial.core.shared.NodeInfo

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

final class Node(nodeId: Long,
                 keyspaceBits: Int,
                 algorithmTimeout: Timeout,
                 externalRequestTimeout: Timeout,
                 eventStream: EventStream)
    extends Actor
    with ActorLogging {

  import Node._
  import Pointers._

  require(keyspaceBits > 0, "keyspaceBits must be a positive Int value")

  private val idModulus = 1 << keyspaceBits

  // Finger table ranges from (nodeId + 2^1) up to (nodeId + 2^(keyspace - 1))
  private def fingerTableSize = keyspaceBits - 1

  private val stabiliseTimeout = Timeout(1500.milliseconds)

  // Check that node ID is reasonable
  require(nodeId >= 0, "ownId must be a non-negative Long value")
  require(nodeId < idModulus, s"ownId must be less than $idModulus (2^$keyspaceBits})")

  private def newPointers(nodeId: Long, seedId: Long, seedRef: ActorRef) =
    context.actorOf(
      Pointers
        .props(nodeId, fingerTableSize, NodeInfo(seedId, seedRef), eventStream)
    )

  private def newCheckPredecessorAlgorithm(nodeRef: ActorRef) =
    context.actorOf(CheckPredecessorAlgorithm.props(nodeRef, externalRequestTimeout))

  private def newStabilisationAlgorithm(pointersRef: ActorRef) =
    context.actorOf(
      StabilisationAlgorithm
        .props(NodeInfo(nodeId, self), pointersRef, externalRequestTimeout)
    )

  private def checkPredecessor(nodeRef: ActorRef, checkPredecessorAlgorithm: ActorRef, replyTo: ActorRef) =
    checkPredecessorAlgorithm
      .ask(CheckPredecessorAlgorithmStart)(algorithmTimeout)
      .mapTo[CheckPredecessorAlgorithmStartResponse]
      .map {
        case CheckPredecessorAlgorithmAlreadyRunning =>
          CheckPredecessorInProgress
        case CheckPredecessorAlgorithmFinished =>
          checkPredecessorAlgorithm ! CheckPredecessorAlgorithmReset(nodeRef, externalRequestTimeout)
          CheckPredecessorOk
        case CheckPredecessorAlgorithmError(message) =>
          checkPredecessorAlgorithm ! CheckPredecessorAlgorithmReset(nodeRef, externalRequestTimeout)
          CheckPredecessorError(message)
      }
      .recover {
        case exception =>
          log.error(exception.getMessage)
          checkPredecessorAlgorithm ! CheckPredecessorAlgorithmReset(nodeRef, externalRequestTimeout)
          CheckPredecessorError("CheckPredecessor request failed due to internal error")
      }
      .pipeTo(replyTo)

  private def closestPrecedingFinger(nodeRef: ActorRef, queryId: Long, replyTo: ActorRef) = {
    val algorithm = context.actorOf(
      ClosestPrecedingNodeAlgorithm
        .props(NodeInfo(nodeId, self), nodeRef, externalRequestTimeout)
    )
    algorithm
      .ask(ClosestPrecedingNodeAlgorithmStart(queryId))(algorithmTimeout)
      .mapTo[ClosestPrecedingNodeAlgorithmStartResponse]
      .map {
        case ClosestPrecedingNodeAlgorithmFinished(finger) =>
          ClosestPrecedingNodeOk(finger)
        case ClosestPrecedingNodeAlgorithmAlreadyRunning =>
          throw new Exception("ClosestPrecedingFingerAlgorithm actor already running")
        case ClosestPrecedingNodeAlgorithmError(message) =>
          ClosestPrecedingNodeError(message)
      }
      .recover {
        case exception => ClosestPrecedingNodeError(exception.getMessage)
      }
      .andThen {
        case _ => algorithm ! PoisonPill
      }
      .pipeTo(replyTo)
  }

  /**
    * Create an actor to execute the FindPredecessor algorithm and, when it finishes/fails, produce a response that
    * will be recognised by the original sender of the request
    *
    * This method passes in the ID and ActorRef of the current node as the initial candidate node, which means the
    * FindPredecessor algorithm will begin its search at the current node.
    */
  private def findPredecessor(queryId: Long, sender: ActorRef): Unit = {
    // The FindPredecessorAlgorithm actor will shutdown immediately after it sends a FindPredecessorAlgorithmOk or
    // FindPredecessorAlgorithmError message. However, if the future returned by the 'ask' request does not complete
    // within the timeout period, the actor must be shutdown manually to ensure that it does not run indefinitely.
    val findPredecessorAlgorithm =
      context.actorOf(FindPredecessorAlgorithm.props())
    findPredecessorAlgorithm
      .ask(FindPredecessorAlgorithmStart(queryId, NodeInfo(nodeId, self)))(algorithmTimeout)
      .mapTo[FindPredecessorAlgorithmStartResponse]
      .map {
        case FindPredecessorAlgorithmOk(predecessor) =>
          FindPredecessorOk(queryId, predecessor)
        case FindPredecessorAlgorithmAlreadyRunning =>
          throw new Exception("FindPredecessorAlgorithm actor already running")
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
  private def findSuccessor(queryId: Long, sender: ActorRef): Unit = {
    // The FindSuccessorAlgorithm actor will shutdown immediately after it sends a FindSuccessorAlgorithmOk or
    // FindSuccessorAlgorithmError message. However, if the future returned by the 'ask' request does not complete
    // within the timeout period, the actor must be shutdown manually to ensure that it does not run indefinitely.
    val findSuccessorAlgorithm = context.actorOf(FindSuccessorAlgorithm.props())
    findSuccessorAlgorithm
      .ask(FindSuccessorAlgorithmStart(queryId, self))(algorithmTimeout)
      .mapTo[FindSuccessorAlgorithmStartResponse]
      .map {
        case FindSuccessorAlgorithmOk(successor) =>
          FindSuccessorOk(queryId, successor)
        case FindSuccessorAlgorithmAlreadyRunning =>
          throw new Exception("FindSuccessorAlgorithm actor already running")
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

  private def fixFingers(sender: ActorRef): Unit =
    sender ! FixFingersError("FixFingers request handler not implemented")

  private def notify(nodeRef: ActorRef, candidate: NodeInfo, replyTo: ActorRef) = {
    val notifyAlgorithm = context.actorOf(NotifyAlgorithm.props())
    notifyAlgorithm
      .ask(NotifyAlgorithmStart(NodeInfo(nodeId, self), candidate, nodeRef))(algorithmTimeout)
      .mapTo[NotifyAlgorithmStartResponse]
      .map {
        case NotifyAlgorithmOk(predecessorUpdated: Boolean) =>
          if (predecessorUpdated) NotifyOk else NotifyIgnored
        case NotifyAlgorithmAlreadyRunning =>
          throw new Exception("NotifyAlgorithm actor already running")
        case NotifyAlgorithmError(message) =>
          NotifyError(message)
      }
      .recover {
        case exception =>
          context.stop(notifyAlgorithm)
          NotifyError(exception.getMessage)
      }
      .pipeTo(replyTo)
  }

  private def scheduleStabilisation(stabilisationAlgorithmRef: ActorRef, nodeRef: ActorRef) =
    context.system.scheduler.schedule(200.milliseconds, 200.milliseconds) {
      stabilisationAlgorithmRef
        .ask(StabilisationAlgorithmStart)(stabiliseTimeout)
        .mapTo[StabilisationAlgorithmStartResponse]
        .map {
          case StabilisationAlgorithmAlreadyRunning =>
            log.warning("Stabilisation already in progress")
          case StabilisationAlgorithmFinished =>
            stabilisationAlgorithmRef ! StabilisationAlgorithmReset(NodeInfo(nodeId, self),
                                                                    nodeRef,
                                                                    externalRequestTimeout)
          case StabilisationAlgorithmError(message) =>
            log.error("Stabilisation failed: {}", message)
            stabilisationAlgorithmRef ! StabilisationAlgorithmReset(NodeInfo(nodeId, self),
                                                                    nodeRef,
                                                                    externalRequestTimeout)
        }
        .recover {
          case exception =>
            log.error("Stabilisation recovery failed: {}", exception.getMessage)
            stabilisationAlgorithmRef ! StabilisationAlgorithmReset(NodeInfo(nodeId, self),
                                                                    nodeRef,
                                                                    externalRequestTimeout)
        }
    }

  def receiveWhileReady(nodeRef: ActorRef,
                        checkPredecessorAlgorithm: ActorRef,
                        stabilisationAlgorithm: ActorRef,
                        stabilisationCancellable: Cancellable): Receive = {
    case CheckPredecessor =>
      checkPredecessor(nodeRef, checkPredecessorAlgorithm, sender())

    case ClosestPrecedingNode(queryId: Long) =>
      closestPrecedingFinger(nodeRef, queryId, sender())

    case m @ GetId =>
      nodeRef.ask(m)(externalRequestTimeout).pipeTo(sender())

    case m @ GetPredecessor =>
      nodeRef.ask(m)(externalRequestTimeout).pipeTo(sender())

    case m @ GetSuccessorList =>
      nodeRef.ask(m)(externalRequestTimeout).pipeTo(sender())

    case FindPredecessor(queryId) =>
      findPredecessor(queryId, sender())

    case FindSuccessor(queryId) =>
      findSuccessor(queryId, sender())

    case FixFingers =>
      fixFingers(sender())

    case Join(seedId, seedRef) =>
      stabilisationCancellable.cancel()

      context.stop(nodeRef)
      context.stop(checkPredecessorAlgorithm)
      context.stop(stabilisationAlgorithm)

      val newPointersRef = newPointers(nodeId, seedId, seedRef)
      val newStabilisationAlgorithmRef = newStabilisationAlgorithm(newPointersRef)
      val newStabilisationCancellable = scheduleStabilisation(newStabilisationAlgorithmRef, newPointersRef)

      context.become(
        receiveWhileReady(
          newPointersRef,
          newCheckPredecessorAlgorithm(newPointersRef),
          newStabilisationAlgorithmRef,
          newStabilisationCancellable
        )
      )

      sender() ! JoinOk

    case Notify(candidateId, candidateRef) =>
      notify(nodeRef, NodeInfo(candidateId, candidateRef), sender())
  }

  override def receive: Receive = {
    val newPointersRef = newPointers(nodeId, nodeId, self)
    val newCheckPredecessorAlgorithmRef = newCheckPredecessorAlgorithm(newPointersRef)
    val newStabilisationAlgorithmRef = newStabilisationAlgorithm(newPointersRef)
    val newStabilisationCancellable = scheduleStabilisation(newStabilisationAlgorithmRef, newPointersRef)

    receiveWhileReady(
      newPointersRef,
      newCheckPredecessorAlgorithmRef,
      newStabilisationAlgorithmRef,
      newStabilisationCancellable
    )
  }
}

object Node {

  sealed trait Request

  sealed trait Response

  case object CheckPredecessor extends Request

  sealed trait CheckPredecessorResponse extends Response

  case object CheckPredecessorInProgress extends CheckPredecessorResponse

  case object CheckPredecessorOk extends CheckPredecessorResponse

  final case class CheckPredecessorError(message: String) extends CheckPredecessorResponse

  final case class ClosestPrecedingNode(queryId: Long) extends Request

  sealed trait ClosestPrecedingNodeResponse extends Response

  final case class ClosestPrecedingNodeOk(node: NodeInfo) extends ClosestPrecedingNodeResponse

  final case class ClosestPrecedingNodeError(message: String) extends ClosestPrecedingNodeResponse

  final case class FindPredecessor(queryId: Long) extends Request

  sealed trait FindPredecessorResponse extends Response

  final case class FindPredecessorOk(queryId: Long, predecessor: NodeInfo) extends FindPredecessorResponse

  final case class FindPredecessorError(queryId: Long, message: String) extends FindPredecessorResponse

  final case class FindSuccessor(queryId: Long) extends Request

  sealed trait FindSuccessorResponse extends Response

  final case class FindSuccessorOk(queryId: Long, successor: NodeInfo) extends FindSuccessorResponse

  final case class FindSuccessorError(queryId: Long, message: String) extends FindSuccessorResponse

  case object FixFingers extends Request

  sealed trait FixFingersResponse extends Response

  case object FixFingersOk extends FixFingersResponse

  case object FixFingersInProgress extends FixFingersResponse

  final case class FixFingersError(message: String) extends FixFingersResponse

  final case class Join(seedId: Long, seedRef: ActorRef) extends Request

  sealed trait JoinResponse extends Response

  case object JoinOk extends JoinResponse

  final case class JoinError(message: String) extends JoinResponse

  final case class Notify(nodeId: Long, nodeRef: ActorRef) extends Request

  sealed trait NotifyResponse extends Response

  case object NotifyOk extends NotifyResponse

  case object NotifyIgnored extends NotifyResponse

  final case class NotifyError(message: String) extends NotifyResponse

  def props(nodeId: Long,
            keyspaceBits: Int,
            algorithmTimeout: Timeout,
            requestTimeout: Timeout,
            eventStream: EventStream): Props =
    Props(new Node(nodeId, keyspaceBits, algorithmTimeout, requestTimeout, eventStream))
}
