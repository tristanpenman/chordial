package com.tristanpenman.chordial.core.actors

import akka.actor.{ActorLogging, Actor, ActorRef, Props}
import com.tristanpenman.chordial.core.Coordinator.{NotifyError, NotifyIgnored, NotifyOk, Notify}
import com.tristanpenman.chordial.core.NodeProtocol._
import com.tristanpenman.chordial.core.shared.{Interval, NodeInfo}

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
 */
class StabilisationAlgorithm extends Actor with ActorLogging {

  import StabilisationAlgorithm._

  def awaitNotify(delegate: ActorRef): Receive = {
    case NotifyOk() =>
      delegate ! StabilisationAlgorithmFinished()
      context.become(receive)

    case NotifyIgnored() =>
      delegate ! StabilisationAlgorithmFinished()
      context.become(receive)

    case NotifyError(message) =>
      delegate ! StabilisationAlgorithmFailed("Successor responded to Notify request with error: $message")
      context.become(receive)

    case StabilisationAlgorithmStart(_, _) =>
      sender() ! StabilisationAlgorithmAlreadyRunning()
  }

  def awaitUpdateSuccessor(delegate: ActorRef, node: NodeInfo, successorRef: ActorRef): Receive = {
    case UpdateSuccessorOk() =>
      successorRef ! Notify(node.id, node.ref)
      context.become(awaitNotify(delegate))

    case StabilisationAlgorithmStart(_, _) =>
      sender() ! StabilisationAlgorithmAlreadyRunning()
  }

  def awaitGetPredecessor(delegate: ActorRef, node: NodeInfo, successor: NodeInfo, innerNodeRef: ActorRef): Receive = {
    case GetPredecessorOk(candidateId, candidateRef) if Interval(node.id + 1, successor.id).contains(candidateId) =>
      innerNodeRef ! UpdateSuccessor(candidateId, candidateRef)
      context.become(awaitUpdateSuccessor(delegate, node, candidateRef))

    case GetPredecessorOk(_, _) | GetPredecessorOkButUnknown() =>
      successor.ref ! Notify(node.id, node.ref)
      context.become(awaitNotify(delegate))

    case StabilisationAlgorithmStart(_, _) =>
      sender() ! StabilisationAlgorithmAlreadyRunning()
  }

  def awaitGetSuccessor(delegate: ActorRef, node: NodeInfo, innerNodeRef: ActorRef): Receive = {
    case GetSuccessorOk(successorId, successorRef) =>
      successorRef ! GetPredecessor()
      context.become(awaitGetPredecessor(delegate, node, NodeInfo(successorId, successorRef), innerNodeRef))

    case StabilisationAlgorithmStart(_, _) =>
      sender() ! StabilisationAlgorithmAlreadyRunning()
  }

  override def receive: Receive = {
    case StabilisationAlgorithmStart(node, innerNodeRef) =>
      innerNodeRef ! GetSuccessor()
      context.become(awaitGetSuccessor(sender(), node, innerNodeRef))
  }
}

object StabilisationAlgorithm {

  case class StabilisationAlgorithmStart(node: NodeInfo, innerNodeRef: ActorRef)

  class StabilisationAlgorithmStartResponse

  case class StabilisationAlgorithmAlreadyRunning() extends StabilisationAlgorithmStartResponse

  case class StabilisationAlgorithmFinished() extends StabilisationAlgorithmStartResponse

  case class StabilisationAlgorithmFailed(message: String) extends StabilisationAlgorithmStartResponse

  def props(): Props = Props(new StabilisationAlgorithm())
}
