package com.tristanpenman.chordial.core.algorithms

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import akka.pattern.{AskTimeoutException, ask}
import akka.util.Timeout
import com.tristanpenman.chordial.core.Node._
import com.tristanpenman.chordial.core.Pointers._
import com.tristanpenman.chordial.core.shared.{Interval, NodeInfo}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.language.postfixOps

/**
 * Actor class that implements the Stabilise algorithm
 *
 * == Algorithm ==
 *
 * The basic stabilisation algorithm is defined in the Chord paper as follows:
 *
 * {{{
 *   n.stabilise()
 *     x = successor.predecessor
 *     if (x IN (n, successor))
 *       successor = x;
 *     successor.notify(n);
 * }}}
 *
 * This implementation includes enhancements described in section E.3 (Failure and Replication) of the Chord paper,
 * that relate to maintaining a list of successor pointers that can be used if one or more of the closest successors
 * fails to respond within a reasonable amount of time. After the closest successor has been determined, its successor
 * list is reconciled with that of the current node.
 *
 * == Messaging semantics ==
 *
 * This actor essentially functions as a state machine for the execution state of the 'stabilisation' algorithm.
 *
 * The actor is either in the 'running' state or the 'ready' state. Sending a \c StabilisationAlgorithmReset message
 * at any time will result in a transition to the 'ready' state, with a new set of arguments. However this will not
 * stop existing invocations of the algorithm from running to completion. \c StabilisationAlgorithmReset messages are
 * idempotent, and will always result in a \c StabilisationAlgorithmResetOk message being returned to the sender.
 *
 * The actor is initially in the 'ready' state, using the arguments provided at construction time.
 *
 * Sending a \c StabilisationAlgorithmStart message will start the algorithm, but only while in the 'ready' state.
 * When the algorithm is in the running state, a \c StabilisationAlgorithmAlreadyRunning message will be returned to
 * the sender. This allows for a certain degree of back-pressure in the client.
 *
 * When the algorithm completes, a \c StabilisationAlgorithmFinished or \c StabilisationAlgorithmError message will be
 * sent to the original sender, depending on the outcome.
 */
class StabilisationAlgorithm(initialNode: NodeInfo, initialPointersRef: ActorRef, initialRequestTimeout: Timeout)
  extends Actor with ActorLogging {

  import StabilisationAlgorithm._

  /**
   * Use the 'Ask' pattern to send a message to the current successor
   *
   * If the successor fails to respond in time, it is assumed to have failed. This function will send a message to the
   * Pointers actor, telling it to remove the failing node from the successor list, replacing it with the first of the
   * backup successors. If there are no backup successors, then an exception will be thrown.
   */
  private def askSuccessor(pointersRef: ActorRef, msg: Any, requestTimeout: Timeout):
  Future[(Any, NodeInfo, Set[Long])] = {
    // Helper function to tell the Pointers actor to discard the primary successor
    def switchToNextSuccessor(backupSuccessors: List[NodeInfo]): Unit = {
      if (backupSuccessors.isEmpty) {
        // If there are no backup successors left, throw an exception
        throw new RuntimeException("No backup successors")
      }

      // Ask node to remove successor from successor list
      val newSuccessor = backupSuccessors.head
      val newBackupSuccessors = backupSuccessors.tail
      Await.ready(pointersRef.ask(UpdateSuccessorList(newSuccessor, newBackupSuccessors))(requestTimeout)
        .mapTo[UpdateSuccessorListOk], Duration.Inf)
    }

    // Helper function to forward a message to the current successor
    def forwardMessage(primarySuccessor: NodeInfo, backupSuccessors: List[NodeInfo], failedNodeIds: Set[Long]):
    Future[(Any, NodeInfo, Set[Long])] =
      primarySuccessor.ref.ask(msg)(requestTimeout)
        .map {
          case m => (m, primarySuccessor, failedNodeIds)
        }
        .recoverWith {
          case ex: AskTimeoutException =>
            switchToNextSuccessor(backupSuccessors)
            forwardMessage(backupSuccessors.head, backupSuccessors.tail, failedNodeIds + primarySuccessor.id)
        }

    pointersRef.ask(GetSuccessorList())(requestTimeout)
      .mapTo[GetSuccessorListResponse]
      .flatMap {
        case GetSuccessorListOk(primarySuccessor, backupSuccessors) =>
          forwardMessage(primarySuccessor, backupSuccessors, Set.empty)
      }
  }

  /**
   * Execute the 'stabilisation' algorithm asynchronously
   *
   * @param node current node
   * @param pointersRef current node's network pointer data
   * @param requestTimeout time to wait on requests to external resources
   *
   * @return a \c Future that will complete once the updated successor has been notified of the current node
   */
  private def runAsync(node: NodeInfo, pointersRef: ActorRef, requestTimeout: Timeout): Future[Unit] = {

    // Step 1:  Get the predecessor of the current node's first live successor
    askSuccessor(pointersRef, GetPredecessor(), requestTimeout)
      .mapTo[(GetPredecessorResponse, NodeInfo, Set[Long])]
      .map {
        case (GetPredecessorOk(candidate), currentSuccessor, failedNodeIds)
          if Interval(node.id + 1, currentSuccessor.id).contains(candidate.id) &&
            ! failedNodeIds.contains(candidate.id) => (candidate, failedNodeIds)
        case (GetPredecessorOk(_), currentSuccessor, failedNodeIds) => (currentSuccessor, failedNodeIds)
        case (GetPredecessorOkButUnknown(), currentSuccessor, failedNodeIds) => (currentSuccessor, failedNodeIds)
      }

    // Step 2:  Reconcile successor list of closest node with that of the current node, removing failed nodes
    .flatMap { case (closestSuccessor, failedNodeIds) =>
      closestSuccessor.ref.ask(GetSuccessorList())(requestTimeout)
        .mapTo[GetSuccessorListResponse]
        .map {
          case GetSuccessorListOk(primarySuccessor, backupSuccessors) =>
            primarySuccessor :: backupSuccessors.dropRight(1)
        }
        .flatMap { backupSuccessors =>
          val backupSuccessorsWithoutFailedNodes =
            backupSuccessors.filterNot { nodeInfo => failedNodeIds.contains(nodeInfo.id) }
          pointersRef.ask(UpdateSuccessorList(closestSuccessor, backupSuccessorsWithoutFailedNodes))(requestTimeout)
            .mapTo[UpdateSuccessorListResponse]
            .map {
              case UpdateSuccessorListOk() => closestSuccessor
            }
        }
    }

    // Step 3:  Notify the new successor that this node presumes to be its predecessor
    .flatMap { newSuccessor =>
      newSuccessor.ref.ask(Notify(node.id, node.ref))(requestTimeout)
        .mapTo[NotifyResponse]
        .map {
          case NotifyOk() | NotifyIgnored() =>
          case NotifyError(message) => throw new Exception(message)
        }
    }
  }

  private def running(): Receive = {
    case StabilisationAlgorithmStart() =>
      sender() ! StabilisationAlgorithmAlreadyRunning()

    case StabilisationAlgorithmReset(newNode, newPointersRef, newRequestTimeout) =>
      context.become(ready(newNode, newPointersRef, newRequestTimeout))
      sender() ! StabilisationAlgorithmReady()
  }

  private def ready(node: NodeInfo, pointersRef: ActorRef, requestTimeout: Timeout): Receive = {
    case StabilisationAlgorithmStart() =>
      val replyTo = sender()
      runAsync(node, pointersRef, requestTimeout).onComplete {
        case util.Success(()) =>
          replyTo ! StabilisationAlgorithmFinished()
        case util.Failure(exception) =>
          replyTo ! StabilisationAlgorithmError(exception.getMessage)
      }
      context.become(running())

    case StabilisationAlgorithmReset(newNode, newPointersRef, newRequestTimeout) =>
      context.become(ready(newNode, newPointersRef, newRequestTimeout))
      sender() ! StabilisationAlgorithmReady()
  }

  override def receive: Receive = ready(initialNode, initialPointersRef, initialRequestTimeout)
}

object StabilisationAlgorithm {

  sealed trait StabilisationAlgorithmRequest

  case class StabilisationAlgorithmStart() extends StabilisationAlgorithmRequest

  case class StabilisationAlgorithmReset(newNode: NodeInfo, newPointersRef: ActorRef, newRequestTimeout: Timeout)
    extends StabilisationAlgorithmRequest

  sealed trait StabilisationAlgorithmStartResponse

  case class StabilisationAlgorithmFinished() extends StabilisationAlgorithmStartResponse

  case class StabilisationAlgorithmAlreadyRunning() extends StabilisationAlgorithmStartResponse

  case class StabilisationAlgorithmError(message: String) extends StabilisationAlgorithmStartResponse

  sealed trait StabilisationAlgorithmResetResponse

  case class StabilisationAlgorithmReady() extends StabilisationAlgorithmResetResponse

  def props(initialNode: NodeInfo, initialPointersRef: ActorRef, initialRequestTimeout: Timeout): Props =
    Props(new StabilisationAlgorithm(initialNode, initialPointersRef, initialRequestTimeout))
}
