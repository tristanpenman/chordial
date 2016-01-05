package com.tristanpenman.chordial.core.actors

import akka.actor._
import akka.pattern.ask
import akka.util.Timeout
import com.tristanpenman.chordial.core.Node._
import com.tristanpenman.chordial.core.shared.NodeInfo

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.language.postfixOps


/**
 * Actor class that implements the CheckPredecessor algorithm
 *
 * The CheckPredecessor algorithm is defined in the Chord paper as follows:
 *
 * {{{
 *   n.check_predecessor()
 *     if (predecessor has failed)
 *       predecessor = nil;
 * }}}
 *
 * This actor functions as a state machine for the execution state of the 'check_predecessor' algorithm.
 *
 * The actor is either in the 'running' state or the 'ready' state. Sending a \c CheckPredecessorAlgorithmReset message
 * at any time will result in a transition to the 'ready' state, with a new set of arguments. However this will not
 * stop existing invocations of the algorithm from running to completion. \c CheckPredecessorAlgorithmReset messages are
 * idempotent, and will always result in a \c CheckPredecessorAlgorithmResetOk message being returned to the sender.
 *
 * The actor is initially in the 'ready' state, using the arguments provided at construction time.
 *
 * Sending a \c CheckPredecessorAlgorithmStart message will start the algorithm, but only while in the 'ready' state.
 * When the algorithm is in the running state, a \c CheckPredecessorAlgorithmAlreadyRunning message will be returned to
 * the sender. This allows for a certain degree of back-pressure in the client.
 *
 * When the algorithm completes, a \c CheckPredecessorAlgorithmFinished or \c CheckPredecessorAlgorithmError message
 * will be sent to the original sender, depending on the outcome.
 */
class CheckPredecessorAlgorithm(initialInnerNodeRef: ActorRef, initialRequestTimeout: Timeout)
  extends Actor with ActorLogging {

  import CheckPredecessorAlgorithm._

  /**
   * Execute the 'check_predecessor' algorithm asynchronously
   *
   * @param innerNodeRef current node's internal link data
   * @param requestTimeout time to wait on requests to external resources
   *
   * @return a \c Future that will complete once the predecessor has been contacted, or its pointer reset
   */
  private def runAsync(innerNodeRef: ActorRef, requestTimeout: Timeout): Future[Unit] = {

    // Step 1: Find predecessor for current node
    innerNodeRef.ask(GetPredecessor())(requestTimeout)
      .mapTo[GetPredecessorResponse]
      .map {
        case GetPredecessorOk(predecessorId, predecessorRef) =>
          Some(NodeInfo(predecessorId, predecessorRef))
        case GetPredecessorOkButUnknown() =>
          None
      }

    // Step 2: Perform GetSuccessor request to decide whether predecessor pointer should be reset
    .flatMap {
      case Some(predecessor) =>
        predecessor.ref.ask(GetSuccessor())(requestTimeout)
          .mapTo[GetSuccessorResponse]
          .map {
            case GetSuccessorOk(_, _) => false   // Predecessor is active
          }
          .recover {
            case exception => true               // Predecessor has failed
          }
      case None =>
        Future { false }                         // Predecessor pointer has not been set
    }

    // Step 3: Reset predecessor pointer if necessary, and wait for acknowledgement
    .flatMap { shouldResetPredecessor =>
      if (shouldResetPredecessor) {
        innerNodeRef.ask(ResetPredecessor())(requestTimeout)
          .mapTo[ResetPredecessorResponse]
          .map {
            case ResetPredecessorOk() =>
          }
      } else {
        Future { }
      }
    }
  }

  private def running(): Receive = {
    case CheckPredecessorAlgorithmStart() =>
      sender() ! CheckPredecessorAlgorithmAlreadyRunning()

    case CheckPredecessorAlgorithmReset(newInnerNodeRef, newRequestTimeout) =>
      context.become(ready(newInnerNodeRef, newRequestTimeout))
      sender() ! CheckPredecessorAlgorithmReady()
  }

  private def ready(innerNodeRef: ActorRef, requestTimeout: Timeout): Receive = {
    case CheckPredecessorAlgorithmStart() =>
      var replyTo = sender()
      runAsync(innerNodeRef, requestTimeout).onComplete {
        case util.Success(()) =>
          replyTo ! CheckPredecessorAlgorithmFinished()
        case util.Failure(exception) =>
          replyTo ! CheckPredecessorAlgorithmError(exception.getMessage)
      }
      context.become(running())

    case CheckPredecessorAlgorithmReset(newInnerNodeRef, newRequestTimeout) =>
      context.become(ready(newInnerNodeRef, newRequestTimeout))
      sender() ! CheckPredecessorAlgorithmReady()
  }

  override def receive: Receive = ready(initialInnerNodeRef, initialRequestTimeout)
}

object CheckPredecessorAlgorithm {

  sealed trait CheckPredecessorAlgorithmRequest

  case class CheckPredecessorAlgorithmStart()

  case class CheckPredecessorAlgorithmReset(newInnerNodeRef: ActorRef, newRequestTimeout: Timeout)
    extends CheckPredecessorAlgorithmRequest

  sealed trait CheckPredecessorAlgorithmStartResponse

  case class CheckPredecessorAlgorithmFinished() extends CheckPredecessorAlgorithmStartResponse

  case class CheckPredecessorAlgorithmAlreadyRunning() extends CheckPredecessorAlgorithmStartResponse

  case class CheckPredecessorAlgorithmError(message: String) extends CheckPredecessorAlgorithmStartResponse

  sealed trait CheckPredecessorAlgorithmResetResponse

  case class CheckPredecessorAlgorithmReady() extends CheckPredecessorAlgorithmResetResponse

  def props(initialInnerNodeRef: ActorRef, initialRequestTimeout: Timeout): Props =
    Props(new CheckPredecessorAlgorithm(initialInnerNodeRef, initialRequestTimeout))

}
