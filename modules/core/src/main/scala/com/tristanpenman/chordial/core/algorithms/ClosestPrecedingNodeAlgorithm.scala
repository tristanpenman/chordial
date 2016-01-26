package com.tristanpenman.chordial.core.algorithms

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import com.tristanpenman.chordial.core.Pointers.{GetSuccessorList, GetSuccessorListOk}
import com.tristanpenman.chordial.core.shared.{Interval, NodeInfo}

/**
 * Actor class that implements a simplified version of the ClosestPrecedingNode algorithm
 *
 * The ClosestPrecedingNode algorithm is defined in the Chord paper as follows:
 *
 * {{{
 *   n.closest_preceding_finger(id)
 *     for i - m downto 1
 *       if (finger[i].node IN (n, id))
 *         return finger[i].node;
 *     return n;
 * }}}
 *
 * The algorithm implemented here behaves as though the node has a finger table of size 2, with the first entry being
 * the node's successor, and the second entry being the node itself.
 */
class ClosestPrecedingNodeAlgorithm extends Actor with ActorLogging {

  import ClosestPrecedingNodeAlgorithm._

  def awaitGetSuccessorList(delegate: ActorRef, queryId: Long, node: NodeInfo): Receive = {
    case GetSuccessorListOk(primarySuccessor, _) =>
      if (Interval(node.id + 1, queryId).contains(primarySuccessor.id)) {
        delegate ! ClosestPrecedingNodeAlgorithmOk(primarySuccessor)
        context.stop(self)
      } else {
        delegate ! ClosestPrecedingNodeAlgorithmOk(node)
        context.stop(self)
      }

    case ClosestPrecedingNodeAlgorithmStart(_, _, _) =>
      sender() ! ClosestPrecedingNodeAlgorithmAlreadyRunning()

    case message =>
      log.warning("Received unexpected message while waiting for StabilisationAlgorithmStart: {}", message)
  }

  override def receive: Receive = {
    case ClosestPrecedingNodeAlgorithmStart(queryId, node, pointersRef) =>
      pointersRef ! GetSuccessorList()
      context.become(awaitGetSuccessorList(sender(), queryId, node))

    case message =>
      log.warning("Received unexpected message while waiting for ClosestPrecedingFingerAlgorithmStart: {}", message)
  }

}

object ClosestPrecedingNodeAlgorithm {

  case class ClosestPrecedingNodeAlgorithmStart(queryId: Long, node: NodeInfo, pointersRef: ActorRef)

  sealed trait ClosestPrecedingNodeAlgorithmStartResponse

  case class ClosestPrecedingNodeAlgorithmAlreadyRunning() extends ClosestPrecedingNodeAlgorithmStartResponse

  case class ClosestPrecedingNodeAlgorithmOk(finger: NodeInfo) extends ClosestPrecedingNodeAlgorithmStartResponse

  case class ClosestPrecedingNodeAlgorithmError(message: String) extends ClosestPrecedingNodeAlgorithmStartResponse

  def props(): Props = Props(new ClosestPrecedingNodeAlgorithm())

}