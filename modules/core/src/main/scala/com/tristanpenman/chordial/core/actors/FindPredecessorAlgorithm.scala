package com.tristanpenman.chordial.core.actors

import akka.actor.{ActorLogging, ActorRef, Actor, Props}
import com.tristanpenman.chordial.core.Coordinator._
import com.tristanpenman.chordial.core.Pointers._
import com.tristanpenman.chordial.core.shared.{NodeInfo, Interval}

/**
 * Actor class that implements the FindPredecessor algorithm
 *
 * The FindPredecessor algorithm is defined in the Chord paper as follows:
 *
 * {{{
 *   n.find_predecessor(id)
 *     n' = n;
 *     while (id NOT_IN (n', n'.successor])
 *       n' = n'.closest_preceding_finger(id);
 *     return n';
 * }}}
 *
 * This algorithm has been implemented as a series of alternating 'successor' and 'closest_preceding_finger'
 * operations, each performed by sending a message to an ActorRef and awaiting an appropriate response.
 *
 * Note that the NOT_IN operator is defined in terms of an interval that wraps around to the minimum value.
 */
class FindPredecessorAlgorithm extends Actor with ActorLogging {

  import FindPredecessorAlgorithm._

  def awaitGetSuccessorList(queryId: Long, delegate: ActorRef, candidate: NodeInfo): Actor.Receive = {
    case GetSuccessorListOk(primarySuccessor: NodeInfo, _) =>
      // Check whether the query ID belongs to the candidate node's successor
      if (Interval(candidate.id + 1, primarySuccessor.id + 1).contains(queryId)) {
        // If the query ID belongs to the candidate node's successor, then we have successfully found the predecessor
        delegate ! FindPredecessorAlgorithmOk(candidate)
        context.stop(self)
      } else {
        // Otherwise, we need to choose the next node by the asking the current candidate node to return what it knows
        // to be the closest preceding finger for the query ID
        candidate.ref ! ClosestPrecedingNode(queryId)
        context.become(awaitClosestPrecedingNode(queryId, delegate))
      }

    case FindPredecessorAlgorithmStart(_, _) =>
      sender() ! FindPredecessorAlgorithmAlreadyRunning()

    case message =>
      log.warning("Received unexpected message while waiting for GetSuccessorResponse: {}", message)
  }

  def awaitClosestPrecedingNode(queryId: Long, delegate: ActorRef): Actor.Receive = {
    case ClosestPrecedingNodeOk(candidate) =>
      // Now that we have the ID and ActorRef for the next candidate node, we can proceed to the next step of the
      // algorithm. This requires that we locate the successor of the candidate node.
      candidate.ref ! GetSuccessorList()
      context.become(awaitGetSuccessorList(queryId, delegate, candidate))

    case ClosestPrecedingNodeError(message: String) =>
      delegate ! FindPredecessorAlgorithmError(s"ClosestPrecedingFinder request failed with message: $message")
      context.stop(self)

    case FindPredecessorAlgorithmStart(_, _) =>
      sender() ! FindPredecessorAlgorithmAlreadyRunning()

    case message =>
      log.warning("Received unexpected message while waiting for ClosestPrecedingFingerResponse: {}", message)
  }

  override def receive: Receive = {
    case FindPredecessorAlgorithmStart(queryId: Long, initialNode: NodeInfo) =>
      initialNode.ref ! GetSuccessorList()
      context.become(awaitGetSuccessorList(queryId, sender(), initialNode))

    case message =>
      log.warning("Received unexpected message while waiting for FindPredecessorAlgorithmStart: {}", message)
  }
}

object FindPredecessorAlgorithm {

  case class FindPredecessorAlgorithmStart(queryId: Long, initialNode: NodeInfo)

  sealed trait FindPredecessorAlgorithmStartResponse

  case class FindPredecessorAlgorithmAlreadyRunning() extends FindPredecessorAlgorithmStartResponse

  case class FindPredecessorAlgorithmOk(predecessor: NodeInfo) extends FindPredecessorAlgorithmStartResponse

  case class FindPredecessorAlgorithmError(message: String) extends FindPredecessorAlgorithmStartResponse

  def props(): Props = Props(new FindPredecessorAlgorithm())

}
