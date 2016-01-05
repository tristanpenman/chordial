package com.tristanpenman.chordial.core.actors

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import akka.pattern.ask
import akka.util.Timeout
import com.tristanpenman.chordial.core.Coordinator._
import com.tristanpenman.chordial.core.Node._
import com.tristanpenman.chordial.core.shared.{Interval, NodeInfo}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.language.postfixOps

/**
 * Actor class that implements the Stabilise algorithm
 *
 * The Stabilise algorithm is defined in the Chord paper as follows:
 *
 * {{{
 *   n.stabilise()
 *     x = successor.predecessor
 *     if (x IN (n, successor))
 *       successor = x;
 *     successor.notify(n);
 * }}}
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
class StabilisationAlgorithm(initialNode: NodeInfo, initialInnerNodeRef: ActorRef, initialRequestTimeout: Timeout)
  extends Actor with ActorLogging {

  import StabilisationAlgorithm._

  /**
   * Execute the 'stabilisation' algorithm asynchronously
   *
   * @param node current node
   * @param innerNodeRef current node's internal link data
   * @param requestTimeout time to wait on requests to external resources
   *
   * @return a \c Future that will complete once the updated successor has been notified of the current node
   */
  private def runAsync(node: NodeInfo, innerNodeRef: ActorRef, requestTimeout: Timeout): Future[Unit] = {

    // Step 1:  Get the successor for the current node
    innerNodeRef.ask(GetSuccessor())(requestTimeout)
      .mapTo[GetSuccessorResponse]
      .map {
        case GetSuccessorOk(successorId, successorRef) => NodeInfo(successorId, successorRef)
      }

    .flatMap { currentSuccessor =>
      // Step 2a:  Get the successor's predecessor node
      currentSuccessor.ref.ask(GetPredecessor())(requestTimeout)
        .mapTo[GetPredecessorResponse]
        .map {
          case GetPredecessorOk(candidateId, candidateRef)
            if Interval(node.id + 1, currentSuccessor.id).contains(candidateId) => NodeInfo(candidateId, candidateRef)
          case GetPredecessorOk(_, _) | GetPredecessorOkButUnknown() => currentSuccessor
        }

      // Step 2b:  Choose the closest candidate successor and update the current node's successor if necessary
      .flatMap { newSuccessor =>
        if (newSuccessor.id == currentSuccessor.id) {
          Future {
            newSuccessor
          }
        } else {
          innerNodeRef.ask(UpdateSuccessor(newSuccessor.id, newSuccessor.ref))(requestTimeout)
            .mapTo[UpdateSuccessorResponse]
            .map {
              case UpdateSuccessorOk() => newSuccessor
              case UpdateSuccessorInvalidRequest(message) => throw new Exception(message)
            }
        }
      }
    }

    // Step 3:  Notify the new successor that this node may be its predecessor
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

    case StabilisationAlgorithmReset(newNode, newInnerNodeRef, newRequestTimeout) =>
      context.become(ready(newNode, newInnerNodeRef, newRequestTimeout))
      sender() ! StabilisationAlgorithmReady()
  }

  private def ready(node: NodeInfo, innerNodeRef: ActorRef, requestTimeout: Timeout): Receive = {
    case StabilisationAlgorithmStart() =>
      val replyTo = sender()
      runAsync(node, innerNodeRef, requestTimeout).onComplete {
        case util.Success(()) =>
          replyTo ! StabilisationAlgorithmFinished()
        case util.Failure(exception) =>
          replyTo ! StabilisationAlgorithmError(exception.getMessage)
      }
      context.become(running())

    case StabilisationAlgorithmReset(newNode, newInnerNodeRef, newRequestTimeout) =>
      context.become(ready(newNode, newInnerNodeRef, newRequestTimeout))
      sender() ! StabilisationAlgorithmReady()
  }

  override def receive: Receive = ready(initialNode, initialInnerNodeRef, initialRequestTimeout)
}

object StabilisationAlgorithm {

  sealed trait StabilisationAlgorithmRequest

  case class StabilisationAlgorithmStart() extends StabilisationAlgorithmRequest

  case class StabilisationAlgorithmReset(newNode: NodeInfo, newInnerNodeRef: ActorRef, newRequestTimeout: Timeout)
    extends StabilisationAlgorithmRequest

  sealed trait StabilisationAlgorithmStartResponse

  case class StabilisationAlgorithmFinished() extends StabilisationAlgorithmStartResponse

  case class StabilisationAlgorithmAlreadyRunning() extends StabilisationAlgorithmStartResponse

  case class StabilisationAlgorithmError(message: String) extends StabilisationAlgorithmStartResponse

  sealed trait StabilisationAlgorithmResetResponse

  case class StabilisationAlgorithmReady() extends StabilisationAlgorithmResetResponse

  def props(initialNode: NodeInfo, initialInnerNodeRef: ActorRef, initialRequestTimeout: Timeout): Props =
    Props(new StabilisationAlgorithm(initialNode, initialInnerNodeRef, initialRequestTimeout))
}
