package com.tristanpenman.chordial.core.actors

import akka.actor.{ActorLogging, ActorRef, Actor, Props}
import com.tristanpenman.chordial.core.NodeProtocol._
import com.tristanpenman.chordial.core.shared.Interval

/**
 * Actor class that implements the FindPredecessor algorithm
 *
 * The FindPredecessor algorithm is defined in the Chord paper as follows:

 *   n.find_predecessor(id)
 *     n' = n;
 *     while (id NOT_IN (n', n'.successor])
 *       n' = n'.closest_preceding_finger(id);
 *     return n';
 *
 * This actor implements this algorithm using the GetSuccessorMonitor and ClosestPrecedingFingerMonitor classes to
 * perform and monitor requests made to other nodes. The main loop is implicitly defined by two contexts, which
 * alternate depending on which kind of monitor we're currently waiting on.
 *
 * The algorithm will fail if either monitor receives a timeout.
 *
 * Note that the NOT_IN operator is defined in terms of an interval that wraps around to the minimum value.
 */
class FindPredecessorAlgorithm extends Actor with ActorLogging {

  import FindPredecessorAlgorithm._

  def awaitGetSuccessor(queryId: Long, delegate: ActorRef, candidateId: Long, candidate: ActorRef): Actor.Receive = {
    case GetSuccessorOk(successorId: Long, _) =>
      // Check whether the query ID belongs to the candidate node's successor
      if (Interval(candidateId + 1, successorId + 1).contains(queryId)) {
        // If the query ID belongs to the candidate node's successor, then we have successfully found the predecessor
        delegate ! FindPredecessorAlgorithmOk(candidateId, candidate)
        context.stop(self)
      } else {
        // Otherwise, we need to choose the next node by the asking the current candidate node to return what it knows
        // to be the closest preceding finger for the query ID
        candidate ! ClosestPrecedingFinger(queryId)
        context.become(awaitClosestPrecedingFinger(queryId, delegate))
      }

    case message =>
      log.warning("Received unexpected message while waiting for GetSuccessor response: {}", message)
  }

  def awaitClosestPrecedingFinger(queryId: Long, delegate: ActorRef): Actor.Receive = {
    case ClosestPrecedingFingerOk(actualQueryId, candidateId, candidateRef) if actualQueryId == queryId =>
      // Now that we have the ID and ActorRef for the next candidate node, we can proceed to the next step of the
      // algorithm. This requires that we locate the successor of the candidate node.
      candidateRef ! GetSuccessor()
      context.become(awaitGetSuccessor(queryId, delegate, candidateId, candidateRef))

    case ClosestPrecedingFingerError(queryId: Long, message: String) =>
      delegate ! FindPredecessorAlgorithmError(s"ClosestPrecedingFinder request failed with message: $message")
      context.stop(self)

    case message =>
      log.warning("Received unexpected message while waiting for ClosestPrecedingFinger response: {}", message)
  }

  override def receive: Receive = {
    case FindPredecessorAlgorithmBegin(queryId: Long, initialNodeId: Long, initialNodeRef: ActorRef) =>
      initialNodeRef ! GetSuccessor()
      context.become(awaitGetSuccessor(queryId, sender(), initialNodeId, initialNodeRef))
  }
}

object FindPredecessorAlgorithm {

  case class FindPredecessorAlgorithmBegin(queryId: Long, initialNodeId: Long, initialNodeRef: ActorRef)

  class FindPredecessorAlgorithmResponse

  case class FindPredecessorAlgorithmOk(predecessorId: Long, predecessor: ActorRef)
    extends FindPredecessorAlgorithmResponse

  case class FindPredecessorAlgorithmError(message: String) extends FindPredecessorAlgorithmResponse

  def props(): Props = Props(new FindPredecessorAlgorithm())

}
