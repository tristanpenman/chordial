package com.tristanpenman.chordial.core.actors

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import akka.pattern.ask
import akka.util.Timeout
import com.tristanpenman.chordial.core.Coordinator._
import com.tristanpenman.chordial.core.Pointers._
import com.tristanpenman.chordial.core.shared.{Interval, NodeInfo}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
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
   * Execute the 'stabilisation' algorithm asynchronously
   *
   * @param node current node
   * @param pointersRef current node's network pointer data
   * @param requestTimeout time to wait on requests to external resources
   *
   * @return a \c Future that will complete once the updated successor has been notified of the current node
   */
  private def runAsync(node: NodeInfo, pointersRef: ActorRef, requestTimeout: Timeout): Future[Unit] = {

    // Step 1:  Get the successor for the current node
    pointersRef.ask(GetSuccessorList())(requestTimeout)
      .mapTo[GetSuccessorListResponse]
      .map {
        case GetSuccessorListOk(primarySuccessor, _) => primarySuccessor
      }

    // Step 2:  Get the successor's predecessor node
    .flatMap { currentSuccessor =>
      currentSuccessor.ref.ask(GetPredecessor())(requestTimeout)
        .mapTo[GetPredecessorResponse]
        .map {
          case GetPredecessorOk(candidate)
            if Interval(node.id + 1, currentSuccessor.id).contains(candidate.id) => candidate
          case GetPredecessorOk(_) | GetPredecessorOkButUnknown() => currentSuccessor
        }
    }

    // Step 3:  Reconcile successor list of closest node with that of the current node
    .flatMap { closestSuccessor =>
      closestSuccessor.ref.ask(GetSuccessorList())(requestTimeout)
        .mapTo[GetSuccessorListResponse]
        .map {
          case GetSuccessorListOk(primarySuccessor, backupSuccessors) =>
            primarySuccessor :: backupSuccessors.dropRight(1)
        }
        .flatMap { backupSuccessors =>
          pointersRef.ask(UpdateSuccessorList(closestSuccessor, backupSuccessors))(requestTimeout)
            .mapTo[UpdateSuccessorListResponse]
            .map {
              case UpdateSuccessorListOk() => closestSuccessor
            }
        }
    }

    // Step 4:  Notify the new successor that this node presumes to be its predecessor
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
