package com.tristanpenman.chordial.core.actors

import akka.actor.{ActorLogging, Actor, ActorRef, Props}
import com.tristanpenman.chordial.core.Node._
import com.tristanpenman.chordial.core.shared.{Interval, NodeInfo}

/**
 * Actor class that implements the Notify algorithm, which forms part of Chord's asynchronous stabilisation protocol
 *
 * The Notify algorithm is defined in the Chord paper as follows:
 *
 * {{{
 *   n.notify(n')
 *     if (predecessor is nil or n' IN (predecessor, n))
 *       predecessor = n';
 * }}}
 */
class NotifyAlgorithm extends Actor with ActorLogging {

  import NotifyAlgorithm._

  def awaitUpdatePredecessor(delegate: ActorRef): Receive = {
    case UpdatePredecessorOk() =>
      delegate ! NotifyAlgorithmOk(true)
      context.stop(self)

    case NotifyAlgorithmStart(_, _, _) =>
      sender() ! NotifyAlgorithmAlreadyRunning()

    case message =>
      log.warning("Received unexpected message while waiting for UpdatePredecessorResponse: {}", message)
  }

  def awaitGetPredecessor(delegate: ActorRef, node: NodeInfo, candidate: NodeInfo, innerNodeRef: ActorRef): Receive = {
    case GetPredecessorOk(predecessor) =>
      if (Interval(predecessor.id + 1, node.id).contains(candidate.id)) {
        innerNodeRef ! UpdatePredecessor(candidate)
        context.become(awaitUpdatePredecessor(delegate))
      } else {
        delegate ! NotifyAlgorithmOk(false)
        context.stop(self)
      }

    case GetPredecessorOkButUnknown() =>
      innerNodeRef ! UpdatePredecessor(candidate)
      context.become(awaitUpdatePredecessor(delegate))

    case NotifyAlgorithmStart(_, _, _) =>
      sender() ! NotifyAlgorithmAlreadyRunning()

    case message =>
      log.warning("Received unexpected message while waiting for GetPredecessorResponse: {}", message)
  }

  override def receive: Receive = {
    case NotifyAlgorithmStart(node, candidate, innerNodeRef) =>
      innerNodeRef ! GetPredecessor()
      context.become(awaitGetPredecessor(sender(), node, candidate, innerNodeRef))

    case message =>
      log.warning("Received unexpected message while waiting for NotifyAlgorithmStart: {}", message)
  }
}

object NotifyAlgorithm {

  case class NotifyAlgorithmStart(node: NodeInfo, candidate: NodeInfo, innerNodeRef: ActorRef)

  sealed trait NotifyAlgorithmStartResponse

  case class NotifyAlgorithmAlreadyRunning() extends NotifyAlgorithmStartResponse

  case class NotifyAlgorithmOk(predecessorUpdated: Boolean) extends NotifyAlgorithmStartResponse

  case class NotifyAlgorithmError(message: String) extends NotifyAlgorithmStartResponse

  def props(): Props = Props(new NotifyAlgorithm())
}
