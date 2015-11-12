package com.tristanpenman.chordial.core.actors

import akka.actor.{ActorRef, Actor, ActorLogging, Props}
import com.tristanpenman.chordial.core.NodeProtocol._
import com.tristanpenman.chordial.core.shared.Interval
import com.tristanpenman.chordial.core.shared.NodeInfo

/**
 * Actor class that implements the Stabilise algorithm
 *
 * The Stabilise algorithm is defined in the Chord paper as follows:
 *
 * n.stabilise()
 *   x = successor.predecessor
 *   if (x IN (n, successor))
 *     successor = x;
 *   successor.notify(n);
 */
class StabilisationAlgorithm extends Actor with ActorLogging {
  import StabilisationAlgorithm._

  def awaitGetPredecessor(requestId: Long, delegate: ActorRef, ownId: Long, successor: NodeInfo): Receive = {
    case GetPredecessorOk(predId, predRef) if Interval(ownId + 1, successor.id).contains(predId) =>
      delegate ! StabilisationAlgorithmOk(NodeInfo(predId, predRef))
      context.stop(self)

    case GetPredecessorOk(_, _) | GetPredecessorOkButUnknown() =>
      delegate ! StabilisationAlgorithmOk(successor)
      context.stop(self)
  }

  override def receive: Receive = {
    case StabilisationAlgorithmBegin(requestId, ownId, successor) =>
      successor.ref ! GetPredecessor()
      context.become(awaitGetPredecessor(requestId, sender(), ownId, successor))
  }
}

object StabilisationAlgorithm {

  case class StabilisationAlgorithmBegin(requestId: Long, ownId: Long, successor: NodeInfo)

  class StabilisationAlgorithmResponse

  case class StabilisationAlgorithmOk(successor: NodeInfo) extends StabilisationAlgorithmResponse

  case class StabilisationAlgorithmError(message: String) extends StabilisationAlgorithmResponse

  def props(): Props = Props(new StabilisationAlgorithm())
}
