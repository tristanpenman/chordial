package com.tristanpenman.chordial.core.actors

import akka.actor._
import com.tristanpenman.chordial.core.Node._

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
 * Liveness is checked by sending a GetSuccessor message to the predecessor
 */
class CheckPredecessorAlgorithm extends Actor {

  import CheckPredecessorAlgorithm._

  private def awaitResetPredecessor(replyTo: ActorRef): Receive = {
    case ResetPredecessorOk() =>
      replyTo ! CheckPredecessorAlgorithmFinished()
      context.become(receive)

    case CheckPredecessorAlgorithmStart(_, _) =>
      sender() ! CheckPredecessorAlgorithmAlreadyRunning()
  }

  private def awaitGetSuccessor(replyTo: ActorRef, innerNodeRef: ActorRef): Receive = {
    case GetSuccessorOk(_, _) =>
      replyTo ! CheckPredecessorAlgorithmFinished()
      context.setReceiveTimeout(Duration.Undefined)
      context.become(receive)

    case ReceiveTimeout =>
      innerNodeRef ! ResetPredecessor()
      context.setReceiveTimeout(Duration.Undefined)
      context.become(awaitResetPredecessor(replyTo))

    case CheckPredecessorAlgorithmStart(_, _) =>
      sender() ! CheckPredecessorAlgorithmAlreadyRunning()
  }

  private def awaitGetPredecessor(replyTo: ActorRef, innerNodeRef: ActorRef,
                                  livenessCheckDuration: Duration): Receive = {
    case GetPredecessorOk(_, predecessorRef) =>
      predecessorRef ! GetSuccessor()
      context.setReceiveTimeout(livenessCheckDuration)
      context.become(awaitGetSuccessor(replyTo, innerNodeRef))

    case GetPredecessorOkButUnknown() =>
      replyTo ! CheckPredecessorAlgorithmFinished()
      context.become(receive)

    case CheckPredecessorAlgorithmStart(_, _) =>
      sender() ! CheckPredecessorAlgorithmAlreadyRunning()
  }

  override def receive: Receive = {
    case CheckPredecessorAlgorithmStart(innerNodeRef, livenessCheckDuration) =>
      innerNodeRef ! GetPredecessor()
      context.become(awaitGetPredecessor(sender(), innerNodeRef, livenessCheckDuration))
  }

}

object CheckPredecessorAlgorithm {

  case class CheckPredecessorAlgorithmStart(innerNodeRef: ActorRef, livenessCheckDuration: Duration)

  class CheckPredecessorAlgorithmStartResponse

  case class CheckPredecessorAlgorithmAlreadyRunning() extends CheckPredecessorAlgorithmStartResponse

  case class CheckPredecessorAlgorithmFinished() extends CheckPredecessorAlgorithmStartResponse

  case class CheckPredecessorAlgorithmFailed(message: String) extends CheckPredecessorAlgorithmStartResponse

  def props(): Props = Props(new CheckPredecessorAlgorithm())

}
