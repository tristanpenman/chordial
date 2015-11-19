package com.tristanpenman.chordial.core.actors

import akka.actor.{ActorLogging, Actor, ActorRef, Props}
import com.tristanpenman.chordial.core.Coordinator.{Notify, NotifyError, NotifyIgnored, NotifyOk}
import com.tristanpenman.chordial.core.Node._
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
      delegate ! StabilisationAlgorithmOk()
      context.become(receive)

    case NotifyIgnored() =>
      delegate ! StabilisationAlgorithmOk()
      context.become(receive)

    case NotifyError(message) =>
      delegate ! StabilisationAlgorithmError("Successor responded to Notify request with error: $message")
      context.become(receive)

    case StabilisationAlgorithmStart(_, _) =>
      sender() ! StabilisationAlgorithmAlreadyRunning()

    case message =>
      log.warning("Received unexpected message while waiting for NotifyResponse: {}", message)
  }

  def awaitUpdateSuccessor(delegate: ActorRef, node: NodeInfo, successorRef: ActorRef): Receive = {
    case UpdateSuccessorOk() =>
      successorRef ! Notify(node.id, node.ref)
      context.become(awaitNotify(delegate))

    case StabilisationAlgorithmStart(_, _) =>
      sender() ! StabilisationAlgorithmAlreadyRunning()

    case message =>
      log.warning("Received unexpected message while waiting for UpdateSuccessorResponse: {}", message)
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

    case message =>
      log.warning("Received unexpected message while waiting for GetPredecessorResponse: {}", message)
  }

  def awaitGetSuccessor(delegate: ActorRef, node: NodeInfo, innerNodeRef: ActorRef): Receive = {
    case GetSuccessorOk(successorId, successorRef) =>
      successorRef ! GetPredecessor()
      context.become(awaitGetPredecessor(delegate, node, NodeInfo(successorId, successorRef), innerNodeRef))

    case StabilisationAlgorithmStart(_, _) =>
      sender() ! StabilisationAlgorithmAlreadyRunning()

    case message =>
      log.warning("Received unexpected message while waiting for StabilisationAlgorithmStart: {}", message)
  }

  override def receive: Receive = {
    case StabilisationAlgorithmStart(node, innerNodeRef) =>
      innerNodeRef ! GetSuccessor()
      context.become(awaitGetSuccessor(sender(), node, innerNodeRef))

    case message =>
      log.warning("Received unexpected message while waiting for StabilisationAlgorithmStart: {}", message)
  }
}

object StabilisationAlgorithm {

  case class StabilisationAlgorithmStart(node: NodeInfo, innerNodeRef: ActorRef)

  sealed trait StabilisationAlgorithmStartResponse

  case class StabilisationAlgorithmAlreadyRunning() extends StabilisationAlgorithmStartResponse

  case class StabilisationAlgorithmOk() extends StabilisationAlgorithmStartResponse

  case class StabilisationAlgorithmError(message: String) extends StabilisationAlgorithmStartResponse

  def props(): Props = Props(new StabilisationAlgorithm())
}
