package com.tristanpenman.chordial.core.algorithms

import akka.actor._
import akka.util.Timeout
import com.tristanpenman.chordial.core.Pointers._

import scala.concurrent.duration.Duration

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
  * The actor is either in the 'running' state or the 'ready' state.
  *
  * Sending a \c CheckPredecessorAlgorithmStart message will start the algorithm, but only while in the 'ready' state.
  * When the algorithm is in the running state, a \c CheckPredecessorAlgorithmAlreadyRunning message will be returned to
  * the sender. This allows for a certain degree of back-pressure in the client.
  *
  * When the algorithm completes, a \c CheckPredecessorAlgorithmFinished or \c CheckPredecessorAlgorithmError message
  * will be sent to the original sender, depending on the outcome.
  */
final class CheckPredecessorAlgorithm(router: ActorRef, pointersRef: ActorRef, requestTimeout: Timeout)
    extends Actor
    with ActorLogging {

  import CheckPredecessorAlgorithm._

  private def awaitResetPredecessor(replyTo: ActorRef): Receive = {
    case CheckPredecessorAlgorithmStart =>
      sender() ! CheckPredecessorAlgorithmAlreadyRunning

    case ResetPredecessorOk =>
      context.become(receive)
      context.setReceiveTimeout(Duration.Undefined)
      replyTo ! CheckPredecessorAlgorithmFinished
  }

  private def awaitGetSuccessor(replyTo: ActorRef): Receive = {
    case CheckPredecessorAlgorithmStart =>
      sender() ! CheckPredecessorAlgorithmAlreadyRunning

    case ReceiveTimeout =>
      // Predecessor has failed
      context.become(awaitResetPredecessor(replyTo))
      pointersRef ! ResetPredecessor

    case _ =>
      context.become(receive)
      context.setReceiveTimeout(Duration.Undefined)
      replyTo ! CheckPredecessorAlgorithmFinished
  }

  private def awaitGetPredecessor(replyTo: ActorRef): Receive = {
    case CheckPredecessorAlgorithmStart =>
      sender() ! CheckPredecessorAlgorithmAlreadyRunning

    case GetPredecessorOk(predecessor) =>
      context.become(awaitGetSuccessor(replyTo))
      predecessor.ref ! GetSuccessor

    case GetPredecessorOkButUnknown =>
      context.become(receive)
      context.setReceiveTimeout(Duration.Undefined)
      replyTo ! CheckPredecessorAlgorithmFinished

    case ReceiveTimeout =>
      context.become(receive)
      context.setReceiveTimeout(Duration.Undefined)
      replyTo ! CheckPredecessorAlgorithmError("timed out")
  }

  override def receive: Receive = {
    case CheckPredecessorAlgorithmStart =>
      context.become(awaitGetPredecessor(sender()))
      context.setReceiveTimeout(requestTimeout.duration)
      pointersRef ! GetPredecessor
  }
}

object CheckPredecessorAlgorithm {

  sealed trait CheckPredecessorAlgorithmRequest

  final case class CheckPredecessorAlgorithmStart() extends CheckPredecessorAlgorithmRequest

  sealed trait CheckPredecessorAlgorithmStartResponse

  case object CheckPredecessorAlgorithmFinished extends CheckPredecessorAlgorithmStartResponse

  case object CheckPredecessorAlgorithmAlreadyRunning extends CheckPredecessorAlgorithmStartResponse

  final case class CheckPredecessorAlgorithmError(message: String) extends CheckPredecessorAlgorithmStartResponse

  sealed trait CheckPredecessorAlgorithmResetResponse

  case object CheckPredecessorAlgorithmReady extends CheckPredecessorAlgorithmResetResponse

  def props(router: ActorRef, pointersRef: ActorRef, requestTimeout: Timeout): Props =
    Props(new CheckPredecessorAlgorithm(router, pointersRef, requestTimeout))
}
