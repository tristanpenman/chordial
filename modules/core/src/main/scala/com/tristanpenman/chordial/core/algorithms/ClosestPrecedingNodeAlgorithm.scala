package com.tristanpenman.chordial.core.algorithms

import akka.actor.{Actor, ActorLogging, ActorRef, Props, ReceiveTimeout}
import akka.util.Timeout
import com.tristanpenman.chordial.core.Pointers.{GetSuccessorList, GetSuccessorListOk}
import com.tristanpenman.chordial.core.shared.{Interval, NodeInfo}

import scala.concurrent.duration.Duration

/**
  * Actor class that implements a simplified version of the ClosestPrecedingNode algorithm
  *
  * The ClosestPrecedingNode algorithm is defined in the Chord paper as follows:
  *
  * {{{
  *   n.closest_preceding_node(id)
  *     for i - m downto 1
  *       if (finger[i].node IN (n, id))
  *         return finger[i].node;
  *     return n;
  * }}}
  *
  * The algorithm implemented here behaves as though the node has a finger table of size 2, with the first entry being
  * the node's successor, and the second entry being the node itself.
  */
final class ClosestPrecedingNodeAlgorithm(node: NodeInfo, pointersRef: ActorRef, extTimeout: Timeout)
    extends Actor
    with ActorLogging {

  import ClosestPrecedingNodeAlgorithm._

  private def running(queryId: Long, replyTo: ActorRef): Receive = {
    case ClosestPrecedingNodeAlgorithmStart(_) =>
      sender() ! ClosestPrecedingNodeAlgorithmAlreadyRunning

    case GetSuccessorListOk(primarySuccessor, _) =>
      context.setReceiveTimeout(Duration.Undefined)
      context.become(receive)
      replyTo ! ClosestPrecedingNodeAlgorithmFinished(
        if (Interval(node.id + 1, queryId).contains(primarySuccessor.id)) primarySuccessor else node)

    case ReceiveTimeout =>
      context.setReceiveTimeout(Duration.Undefined)
      context.become(receive)
      replyTo ! ClosestPrecedingNodeAlgorithmError("timed out")
  }

  override def receive: Receive = {
    case ClosestPrecedingNodeAlgorithmStart(queryId: Long) =>
      context.become(running(queryId, sender()))
      context.setReceiveTimeout(extTimeout.duration)
      pointersRef ! GetSuccessorList

    case ReceiveTimeout =>
      // timeout from an earlier request that was completed before the timeout message was processed
      log.debug("late timeout")
  }
}

object ClosestPrecedingNodeAlgorithm {

  sealed trait ClosestPrecedingNodeAlgorithmRequest

  final case class ClosestPrecedingNodeAlgorithmStart(queryId: Long) extends ClosestPrecedingNodeAlgorithmRequest

  sealed trait ClosestPrecedingNodeAlgorithmStartResponse

  final case class ClosestPrecedingNodeAlgorithmFinished(finger: NodeInfo)
      extends ClosestPrecedingNodeAlgorithmStartResponse

  case object ClosestPrecedingNodeAlgorithmAlreadyRunning extends ClosestPrecedingNodeAlgorithmStartResponse

  final case class ClosestPrecedingNodeAlgorithmError(message: String)
      extends ClosestPrecedingNodeAlgorithmStartResponse

  sealed trait ClosestPrecedingNodeAlgorithmResetResponse

  case object ClosestPrecedingNodeAlgorithmReady extends ClosestPrecedingNodeAlgorithmResetResponse

  def props(node: NodeInfo, pointersRef: ActorRef, extTimeout: Timeout): Props =
    Props(new ClosestPrecedingNodeAlgorithm(node, pointersRef, extTimeout))

}
