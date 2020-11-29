package com.tristanpenman.chordial.core.algorithms

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
final class FindSuccessorAlgorithm(router: ActorRef) extends Actor with ActorLogging {

  import FindSuccessorAlgorithm._

  def awaitGetSuccessor(delegate: ActorRef): Receive = {
    case GetSuccessorOk(successor) =>
      delegate ! FindSuccessorAlgorithmOk(successor)
      context.stop(self)

    case FindSuccessorAlgorithmStart(_, _) =>
      sender() ! FindSuccessorAlgorithmAlreadyRunning

    case message =>
      log.warning("Received unexpected message while waiting for GetSuccessorResponse: {}", message)
  }

  def awaitFindPredecessor(delegate: ActorRef): Receive = {
    case FindPredecessorOk(_, predecessor) =>
      predecessor.ref ! GetSuccessor
      context.become(awaitGetSuccessor(delegate))

    case FindPredecessorError(_, message) =>
      delegate ! FindSuccessorAlgorithmError(message)
      context.stop(self)

    case FindSuccessorAlgorithmStart(_, _) =>
      sender() ! FindSuccessorAlgorithmAlreadyRunning

    case message =>
      log.warning("Received unexpected message while waiting for FindPredecessorResponse: {}", message)
  }

  override def receive: Receive = {
    case FindSuccessorAlgorithmStart(queryId: Long, node: ActorRef) =>
      node ! FindPredecessor(queryId)
      context.become(awaitFindPredecessor(sender()))

    case message =>
      log.warning("Received unexpected message while waiting for FindSuccessorAlgorithmStart: {}", message)
  }
}

object FindSuccessorAlgorithm {

  final case class FindSuccessorAlgorithmStart(queryId: Long, initialNodeRef: ActorRef)

  sealed trait FindSuccessorAlgorithmStartResponse

  case object FindSuccessorAlgorithmAlreadyRunning extends FindSuccessorAlgorithmStartResponse

  final case class FindSuccessorAlgorithmOk(successor: NodeInfo) extends FindSuccessorAlgorithmStartResponse

  final case class FindSuccessorAlgorithmError(message: String) extends FindSuccessorAlgorithmStartResponse

  def props(router: ActorRef): Props = Props(new FindSuccessorAlgorithm(router))
}
