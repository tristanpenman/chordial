package com.tristanpenman.chordial.core.actors

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import com.tristanpenman.chordial.core.Node.{FindPredecessor, FindPredecessorError, FindPredecessorOk}
import com.tristanpenman.chordial.core.Pointers._
import com.tristanpenman.chordial.core.shared.NodeInfo

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

  def awaitGetSuccessorList(delegate: ActorRef): Receive = {
    case GetSuccessorListOk(primarySuccessor, _) =>
      delegate ! FindSuccessorAlgorithmOk(primarySuccessor)
      context.stop(self)

    case FindSuccessorAlgorithmStart(_, _) =>
      sender() ! FindSuccessorAlgorithmAlreadyRunning()

    case message =>
      log.warning("Received unexpected message while waiting for GetSuccessorResponse: {}", message)
  }

  def awaitFindPredecessor(delegate: ActorRef): Receive = {
    case FindPredecessorOk(queryId, predecessor) =>
      predecessor.ref ! GetSuccessorList()
      context.become(awaitGetSuccessorList(delegate))

    case FindPredecessorError(queryId, message) =>
      delegate ! FindSuccessorAlgorithmError(message)
      context.stop(self)

    case FindSuccessorAlgorithmStart(_, _) =>
      sender() ! FindSuccessorAlgorithmAlreadyRunning()

    case message =>
      log.warning("Received unexpected message while waiting for FindPredecessorResponse: {}", message)
  }

  override def receive: Receive = {
    case FindSuccessorAlgorithmStart(queryId: Long, initialNodeRef: ActorRef) =>
      initialNodeRef ! FindPredecessor(queryId)
      context.become(awaitFindPredecessor(sender()))

    case message =>
      log.warning("Received unexpected message while waiting for FindSuccessorAlgorithmStart: {}", message)
  }
}

object FindSuccessorAlgorithm {

  case class FindSuccessorAlgorithmStart(queryId: Long, initialNodeRef: ActorRef)

  sealed trait FindSuccessorAlgorithmStartResponse

  case class FindSuccessorAlgorithmAlreadyRunning() extends FindSuccessorAlgorithmStartResponse

  case class FindSuccessorAlgorithmOk(successor: NodeInfo) extends FindSuccessorAlgorithmStartResponse

  case class FindSuccessorAlgorithmError(message: String) extends FindSuccessorAlgorithmStartResponse

  def props(): Props = Props(new FindSuccessorAlgorithm())
}
