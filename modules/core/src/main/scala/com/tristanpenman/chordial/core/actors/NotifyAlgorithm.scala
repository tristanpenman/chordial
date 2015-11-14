package com.tristanpenman.chordial.core.actors

import akka.actor.{Actor, ActorRef, Props}
import com.tristanpenman.chordial.core.NodeProtocol._
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
class NotifyAlgorithm extends Actor {

  import NotifyAlgorithm._

  def awaitUpdatePredecessor(delegate: ActorRef): Receive = {
    case UpdatePredecessorOk() =>
      delegate ! NotifyAlgorithmFinished(true)
      context.stop(self)

    case NotifyAlgorithmStart(_, _, _) =>
      sender() ! NotifyAlgorithmAlreadyRunning()
  }

  def awaitGetPredecessor(delegate: ActorRef, node: NodeInfo, candidate: NodeInfo, innerNodeRef: ActorRef): Receive = {
    case GetPredecessorOk(predecessorId, predecessorRef) =>
      if (Interval(predecessorId + 1, node.id).contains(candidate.id)) {
        innerNodeRef ! UpdatePredecessor(candidate.id, candidate.ref)
        context.become(awaitUpdatePredecessor(delegate))
      } else {
        delegate ! NotifyAlgorithmFinished(false)
        context.stop(self)
      }

    case GetPredecessorOkButUnknown() =>
      innerNodeRef ! UpdatePredecessor(candidate.id, candidate.ref)
      context.become(awaitUpdatePredecessor(delegate))

    case NotifyAlgorithmStart(_, _, _) =>
      sender() ! NotifyAlgorithmAlreadyRunning()
  }

  override def receive: Receive = {
    case NotifyAlgorithmStart(node, candidate, innerNodeRef) =>
      innerNodeRef ! GetPredecessor()
      context.become(awaitGetPredecessor(sender(), node, candidate, innerNodeRef))
  }
}

object NotifyAlgorithm {

  case class NotifyAlgorithmStart(node: NodeInfo, candidate: NodeInfo, innerNodeRef: ActorRef)

  class NotifyAlgorithmResponse

  case class NotifyAlgorithmAlreadyRunning() extends NotifyAlgorithmResponse

  case class NotifyAlgorithmFinished(predecessorUpdated: Boolean) extends NotifyAlgorithmResponse

  def props(): Props = Props(new NotifyAlgorithm())
}
