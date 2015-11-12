package com.tristanpenman.chordial.core.actors

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import com.tristanpenman.chordial.core.NodeProtocol._

/**
 * Actor class that implements the FindSuccessor algorithm
 *
 * The FindSuccessor algorithm is defined in the Chord paper as follows:
 *
 * {{{
 *   n.find_successor(id)
 *     n' = find_predecessor(id)
 *     return n'.successor;
 * }}}
 *
 * Although the algorithm is defined a way that allows 'find_predecessor' to be performed as an ordinary method call,
 * this class performs the operation by sending a message to an ActorRef and awaiting a response.
 */
class FindSuccessorAlgorithm extends Actor with ActorLogging {

  import FindSuccessorAlgorithm._

  def awaitGetSuccessor(delegate: ActorRef): Receive = {
    case GetSuccessorOk(successorId, successorRef: ActorRef) =>
      delegate ! FindSuccessorAlgorithmOk(successorId, successorRef)
      context.stop(self)

    case message =>
      log.warning("Received unexpected message while waiting for GetSuccessorResponse: {}", message)
  }

  def awaitFindPredecessor(delegate: ActorRef): Receive = {
    case FindPredecessorOk(queryId, predecessorId, predecessorRef) =>
      predecessorRef ! GetSuccessor()
      context.become(awaitGetSuccessor(delegate))

    case FindPredecessorError(queryId, message) =>
      delegate ! FindSuccessorAlgorithmError(message)
      context.stop(self)

    case message =>
      log.warning("Received unexpected message while waiting for FindPredecessorResponse: {}", message)
  }

  override def receive: Receive = {
    case FindSuccessorAlgorithmBegin(queryId: Long, initialNodeRef: ActorRef) =>
      initialNodeRef ! FindPredecessor(queryId)
      context.become(awaitFindPredecessor(sender()))

    case message =>
      log.warning("Received unexpected message while waiting for FindSuccessorAlgorithmBegin: {}", message)
  }
}

object FindSuccessorAlgorithm {

  case class FindSuccessorAlgorithmBegin(queryId: Long, initialNodeRef: ActorRef)

  class FindSuccessorAlgorithmResponse

  case class FindSuccessorAlgorithmOk(successorId: Long, successorRef: ActorRef)
    extends FindSuccessorAlgorithmResponse

  case class FindSuccessorAlgorithmError(message: String) extends FindSuccessorAlgorithmResponse

  def props(): Props = Props(new FindSuccessorAlgorithm())
}
