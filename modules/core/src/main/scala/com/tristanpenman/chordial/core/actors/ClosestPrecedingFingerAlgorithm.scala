package com.tristanpenman.chordial.core.actors

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import com.tristanpenman.chordial.core.Node.{GetSuccessor, GetSuccessorOk}
import com.tristanpenman.chordial.core.shared.{Interval, NodeInfo}

/**
 * Actor class that implements a simplified version of the ClosestPrecedingFinger algorithm
 *
 * The ClosestPrecedingFinger algorithm is defined in the Chord paper as follows:
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
class ClosestPrecedingFingerAlgorithm extends Actor with ActorLogging {

  import ClosestPrecedingFingerAlgorithm._

  def awaitGetSuccessor(delegate: ActorRef, queryId: Long, node: NodeInfo): Receive = {
    case GetSuccessorOk(successorId, successorRef) =>
      if (Interval(node.id + 1, queryId).contains(successorId)) {
        delegate ! ClosestPrecedingFingerAlgorithmOk(successorId, successorRef)
        context.stop(self)
      } else {
        delegate ! ClosestPrecedingFingerAlgorithmOk(node.id, node.ref)
        context.stop(self)
      }

    case ClosestPrecedingFingerAlgorithmStart(_, _, _) =>
      sender() ! ClosestPrecedingFingerAlgorithmAlreadyRunning()

    case message =>
      log.warning("Received unexpected message while waiting for StabilisationAlgorithmStart: {}", message)
  }

  override def receive: Receive = {
    case ClosestPrecedingFingerAlgorithmStart(queryId, node, innerNodeRef) =>
      innerNodeRef ! GetSuccessor()
      context.become(awaitGetSuccessor(sender(), queryId, node))

    case message =>
      log.warning("Received unexpected message while waiting for ClosestPrecedingFingerAlgorithmStart: {}", message)
  }

}

object ClosestPrecedingFingerAlgorithm {

  case class ClosestPrecedingFingerAlgorithmStart(queryId: Long, node: NodeInfo, innerNodeRef: ActorRef)

  sealed trait ClosestPrecedingFingerAlgorithmStartResponse

  case class ClosestPrecedingFingerAlgorithmAlreadyRunning() extends ClosestPrecedingFingerAlgorithmStartResponse

  case class ClosestPrecedingFingerAlgorithmOk(fingerId: Long, fingerRef: ActorRef)
    extends ClosestPrecedingFingerAlgorithmStartResponse

  case class ClosestPrecedingFingerAlgorithmError(message: String) extends ClosestPrecedingFingerAlgorithmStartResponse

  def props(): Props = Props(new ClosestPrecedingFingerAlgorithm())

}