package com.tristanpenman.chordial.core.algorithms

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import akka.pattern.ask
import akka.util.Timeout
import com.tristanpenman.chordial.core.Pointers.{GetSuccessorListOk, GetSuccessorListResponse, GetSuccessorList}
import com.tristanpenman.chordial.core.shared.{Interval, NodeInfo}

import scala.concurrent.Future

import scala.concurrent.ExecutionContext.Implicits.global

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
class ClosestPrecedingNodeAlgorithm(initialQueryId: Long, initialNode: NodeInfo, initialPointersRef: ActorRef,
                                    initialExtTimeout: Timeout)
  extends Actor with ActorLogging {

  import ClosestPrecedingNodeAlgorithm._

  /**
   * Execute the 'closest_preceding_node' algorithm asynchronously
   *
   * @param pointersRef current node's internal link data
   * @param extTimeout time to wait on requests to external resources
   *
   * @return a \c Future that will complete once the closest preceding node has been determined
   */
  private def runAsync(queryId: Long, node: NodeInfo, pointersRef: ActorRef, extTimeout: Timeout): Future[NodeInfo] = {
    pointersRef.ask(GetSuccessorList())(extTimeout)
      .mapTo[GetSuccessorListResponse]
      .map {
        case GetSuccessorListOk(primarySuccessor, _) =>
          if (Interval(node.id + 1, queryId).contains(primarySuccessor.id)) {
            primarySuccessor
          } else {
            node
          }
      }
  }

  private def running(): Receive = {
    case ClosestPrecedingNodeAlgorithmStart() =>
      sender() ! ClosestPrecedingNodeAlgorithmAlreadyRunning()

    case ClosestPrecedingNodeAlgorithmReset(newQueryId, newNodeId, newPointersRef, newExtTimeout) =>
      context.become(ready(newQueryId, newNodeId, newPointersRef, newExtTimeout))
      sender() ! ClosestPrecedingNodeAlgorithmReady()
  }

  private def ready(queryId: Long, node: NodeInfo, pointersRef: ActorRef, requestTimeout: Timeout): Receive = {
    case ClosestPrecedingNodeAlgorithmStart() =>
      val replyTo = sender()
      context.become(running())
      runAsync(queryId, node, pointersRef, requestTimeout).onComplete {
        case util.Success(finger) =>
          replyTo ! ClosestPrecedingNodeAlgorithmFinished(finger)
        case util.Failure(exception) =>
          replyTo ! ClosestPrecedingNodeAlgorithmError(exception.getMessage)
      }

    case ClosestPrecedingNodeAlgorithmReset(newQueryId, newNode, newPointersRef, newExtTimeout) =>
      context.become(ready(newQueryId, newNode, newPointersRef, newExtTimeout))
      sender() ! ClosestPrecedingNodeAlgorithmReady()
  }

  override def receive: Receive = ready(initialQueryId, initialNode, initialPointersRef, initialExtTimeout)
}

object ClosestPrecedingNodeAlgorithm {

  sealed trait ClosestPrecedingNodeAlgorithmRequest

  case class ClosestPrecedingNodeAlgorithmStart() extends ClosestPrecedingNodeAlgorithmRequest

  case class ClosestPrecedingNodeAlgorithmReset(queryId: Long, node: NodeInfo, pointersRef: ActorRef,
                                                extTimeout: Timeout)
    extends ClosestPrecedingNodeAlgorithmRequest

  sealed trait ClosestPrecedingNodeAlgorithmStartResponse

  case class ClosestPrecedingNodeAlgorithmFinished(finger: NodeInfo) extends ClosestPrecedingNodeAlgorithmStartResponse

  case class ClosestPrecedingNodeAlgorithmAlreadyRunning() extends ClosestPrecedingNodeAlgorithmStartResponse

  case class ClosestPrecedingNodeAlgorithmError(message: String) extends ClosestPrecedingNodeAlgorithmStartResponse

  sealed trait ClosestPrecedingNodeAlgorithmResetResponse

  case class ClosestPrecedingNodeAlgorithmReady() extends ClosestPrecedingNodeAlgorithmResetResponse

  def props(initialQueryId: Long, initialNode: NodeInfo, initialPointersRef: ActorRef,
            initialExtTimeout: Timeout): Props =
    Props(new ClosestPrecedingNodeAlgorithm(initialQueryId, initialNode, initialPointersRef, initialExtTimeout))

}
