package com.tristanpenman.chordial.core.actors

import akka.actor.{Actor, ActorRef, Props}
import com.tristanpenman.chordial.core.NodeProtocol._
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
class StabilisationAlgorithm extends Actor {

  import StabilisationAlgorithm._

  def awaitGetPredecessor(delegate: ActorRef, nodeId: Long, successor: NodeInfo): Receive = {
    case GetPredecessorOk(candidateId, candidateRef) if Interval(nodeId + 1, successor.id).contains(candidateId) =>
      delegate ! StabilisationAlgorithmFinished(NodeInfo(candidateId, candidateRef))
      context.become(receive)

    case GetPredecessorOk(_, _) | GetPredecessorOkButUnknown() =>
      delegate ! StabilisationAlgorithmFinished(successor)
      context.become(receive)
  }

  def awaitGetSuccessor(delegate: ActorRef, nodeId: Long): Receive = {
    case GetSuccessorOk(successorId, successorRef) =>
      successorRef ! GetPredecessor()
      context.become(awaitGetPredecessor(delegate, nodeId, NodeInfo(successorId, successorRef)))
  }

  override def receive: Receive = {
    case StabilisationAlgorithmStart(node) =>
      node.ref ! GetSuccessor()
      context.become(awaitGetSuccessor(sender(), node.id))
  }
}

object StabilisationAlgorithm {

  case class StabilisationAlgorithmStart(node: NodeInfo)

  case class StabilisationAlgorithmFinished(successor: NodeInfo)

  def props(): Props = Props(new StabilisationAlgorithm())
}
