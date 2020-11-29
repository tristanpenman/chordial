package com.tristanpenman.chordial.core.algorithms

import akka.actor.{Actor, ActorLogging, ActorRef, Props, ReceiveTimeout}
import akka.util.Timeout
import com.tristanpenman.chordial.core.Node._
import com.tristanpenman.chordial.core.Pointers._
import com.tristanpenman.chordial.core.shared.{Interval, NodeInfo}

import scala.concurrent.duration.Duration

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
  * The actor is either in the 'running' state or the 'ready' state.
  *
  * Sending a \c StabilisationAlgorithmStart message will start the algorithm, but only while in the 'ready' state.
  * When the algorithm is in the running state, a \c StabilisationAlgorithmAlreadyRunning message will be returned to
  * the sender. This allows for a certain degree of back-pressure in the client.
  *
  * When the algorithm completes, a \c StabilisationAlgorithmFinished or \c StabilisationAlgorithmError message will be
  * sent to the original sender, depending on the outcome.
  */
final class StabilisationAlgorithm(router: ActorRef, node: NodeInfo, pointersRef: ActorRef, requestTimeout: Timeout)
    extends Actor
    with ActorLogging {

  import StabilisationAlgorithm._

  private def awaitNotify(replyTo: ActorRef): Receive = {
    case StabilisationAlgorithmStart =>
      sender() ! StabilisationAlgorithmAlreadyRunning

    case NotifyOk | NotifyIgnored =>
      context.setReceiveTimeout(Duration.Undefined)
      context.become(receive)
      replyTo ! StabilisationAlgorithmFinished

    case ReceiveTimeout =>
      // TODO: Figure whether/how to handle this better
      context.setReceiveTimeout(Duration.Undefined)
      context.become(receive)
      replyTo ! StabilisationAlgorithmError("Notify timed out")
  }

  private def awaitUpdateSuccessor(replyTo: ActorRef, successor: ActorRef): Receive = {
    case StabilisationAlgorithmStart =>
      sender() ! StabilisationAlgorithmAlreadyRunning

    case UpdateSuccessorOk =>
      context.become(awaitNotify(replyTo))
      successor ! Notify(node.id, node.addr, node.ref)

    case ReceiveTimeout =>
      context.setReceiveTimeout(Duration.Undefined)
      context.become(receive)
      replyTo ! StabilisationAlgorithmError("UpdateSuccessor timed out")
  }

  private def awaitGetPredecessor(replyTo: ActorRef, successor: NodeInfo): Receive = {
    case StabilisationAlgorithmStart =>
      sender() ! StabilisationAlgorithmAlreadyRunning

    case GetPredecessorOk(candidate) if Interval(node.id + 1, successor.id).contains(candidate.id) =>
      context.become(awaitUpdateSuccessor(replyTo, candidate.ref))
      pointersRef ! UpdateSuccessor(candidate)

    case GetPredecessorOk(_) | GetPredecessorOkButUnknown =>
      context.become(awaitNotify(replyTo))
      successor.ref ! Notify(node.id, node.addr, node.ref)

    case ReceiveTimeout =>
      context.setReceiveTimeout(Duration.Undefined)
      context.become(receive)
      replyTo ! StabilisationAlgorithmError("GetPredecessor timed out")
  }

  private def awaitGetSuccessor(replyTo: ActorRef): Receive = {
    case StabilisationAlgorithmStart =>
      sender() ! StabilisationAlgorithmAlreadyRunning

    case GetSuccessorOk(primarySuccessor) =>
      context.become(awaitGetPredecessor(replyTo, primarySuccessor))
      primarySuccessor.ref ! GetPredecessor

    case ReceiveTimeout =>
      // TODO: Attempt to find new successor using finger table
      context.setReceiveTimeout(Duration.Undefined)
      context.become(receive)
      replyTo ! StabilisationAlgorithmError("GetSuccessor timed out")
  }

  override def receive: Receive = {
    case StabilisationAlgorithmStart =>
      context.setReceiveTimeout(requestTimeout.duration)
      context.become(awaitGetSuccessor(sender()))
      pointersRef ! GetSuccessor
  }
}

object StabilisationAlgorithm {

  sealed trait StabilisationAlgorithmRequest

  case object StabilisationAlgorithmStart extends StabilisationAlgorithmRequest

  sealed trait StabilisationAlgorithmStartResponse

  case object StabilisationAlgorithmFinished extends StabilisationAlgorithmStartResponse

  case object StabilisationAlgorithmAlreadyRunning extends StabilisationAlgorithmStartResponse

  final case class StabilisationAlgorithmError(message: String) extends StabilisationAlgorithmStartResponse

  def props(router: ActorRef, node: NodeInfo, pointersRef: ActorRef, requestTimeout: Timeout): Props =
    Props(new StabilisationAlgorithm(router, node, pointersRef, requestTimeout))
}
