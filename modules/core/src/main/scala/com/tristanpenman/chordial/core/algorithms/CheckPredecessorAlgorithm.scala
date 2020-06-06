package com.tristanpenman.chordial.core.algorithms

import akka.actor._
import akka.pattern.ask
import akka.util.Timeout
import com.tristanpenman.chordial.core.Pointers._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

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
final class CheckPredecessorAlgorithm(initialPointersRef: ActorRef, initialRequestTimeout: Timeout)
    extends Actor
    with ActorLogging {

  import CheckPredecessorAlgorithm._

  /**
    * Execute the 'check_predecessor' algorithm asynchronously
    *
    * @param pointersRef current node's internal link data
    * @param requestTimeout time to wait on requests to external resources
    *
    * @return a \c Future that will complete once the predecessor has been contacted, or its pointer reset
    */
  private def runAsync(pointersRef: ActorRef, requestTimeout: Timeout): Future[Unit] =
    // Step 1: Find predecessor for current node
    pointersRef
      .ask(GetPredecessor)(requestTimeout)
      .mapTo[GetPredecessorResponse]
      .map {
        case GetPredecessorOk(predecessor) =>
          Some(predecessor)
        case GetPredecessorOkButUnknown =>
          None
      }

      // Step 2: Perform GetSuccessor request to decide whether predecessor pointer should be reset
      .flatMap {
        case Some(predecessor) =>
          predecessor.ref
            .ask(GetSuccessorList)(requestTimeout)
            .mapTo[GetSuccessorListResponse]
            .map {
              case GetSuccessorListOk(_, _) => false // Predecessor is active
            }
            .recover {
              case _ => true // Predecessor has failed
            }
        case None =>
          Future { false } // Predecessor pointer has not been set
      }

      // Step 3: Reset predecessor pointer if necessary, and wait for acknowledgement
      .flatMap { shouldResetPredecessor =>
        if (shouldResetPredecessor) {
          pointersRef
            .ask(ResetPredecessor)(requestTimeout)
            .mapTo[ResetPredecessorResponse]
            .map {
              case ResetPredecessorOk =>
            }
        } else {
          Future {}
        }
      }

  private def running(): Receive = {
    case CheckPredecessorAlgorithmStart =>
      sender() ! CheckPredecessorAlgorithmAlreadyRunning

    case CheckPredecessorAlgorithmReset(newPointersRef, newRequestTimeout) =>
      context.become(ready(newPointersRef, newRequestTimeout))
      sender() ! CheckPredecessorAlgorithmReady
  }

  private def ready(pointersRef: ActorRef, requestTimeout: Timeout): Receive = {
    case CheckPredecessorAlgorithmStart =>
      val replyTo = sender()
      context.become(running())
      runAsync(pointersRef, requestTimeout).onComplete {
        case util.Success(()) =>
          replyTo ! CheckPredecessorAlgorithmFinished
        case util.Failure(exception) =>
          replyTo ! CheckPredecessorAlgorithmError(exception.getMessage)
      }

    case CheckPredecessorAlgorithmReset(newPointersRef, newRequestTimeout) =>
      context.become(ready(newPointersRef, newRequestTimeout))
      sender() ! CheckPredecessorAlgorithmReady
  }

  override def receive: Receive =
    ready(initialPointersRef, initialRequestTimeout)
}

object CheckPredecessorAlgorithm {

  sealed trait CheckPredecessorAlgorithmRequest

  final case class CheckPredecessorAlgorithmStart() extends CheckPredecessorAlgorithmRequest

  final case class CheckPredecessorAlgorithmReset(newPointersRef: ActorRef, newRequestTimeout: Timeout)
      extends CheckPredecessorAlgorithmRequest

  sealed trait CheckPredecessorAlgorithmStartResponse

  case object CheckPredecessorAlgorithmFinished extends CheckPredecessorAlgorithmStartResponse

  case object CheckPredecessorAlgorithmAlreadyRunning extends CheckPredecessorAlgorithmStartResponse

  final case class CheckPredecessorAlgorithmError(message: String) extends CheckPredecessorAlgorithmStartResponse

  sealed trait CheckPredecessorAlgorithmResetResponse

  case object CheckPredecessorAlgorithmReady extends CheckPredecessorAlgorithmResetResponse

  def props(initialPointersRef: ActorRef, initialRequestTimeout: Timeout): Props =
    Props(new CheckPredecessorAlgorithm(initialPointersRef, initialRequestTimeout))

}
