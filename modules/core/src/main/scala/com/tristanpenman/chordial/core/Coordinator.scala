package com.tristanpenman.chordial.core

import akka.actor._
import akka.pattern.{ask, pipe}
import akka.util.Timeout
import com.tristanpenman.chordial.core.NodeProtocol._
import com.tristanpenman.chordial.core.actors.FindPredecessorAlgorithm._
import com.tristanpenman.chordial.core.actors.FindSuccessorAlgorithm._
import com.tristanpenman.chordial.core.actors.NotifyAlgorithm.{NotifyAlgorithmFinished, NotifyAlgorithmResponse, NotifyAlgorithmStart}
import com.tristanpenman.chordial.core.actors.StabilisationAlgorithm._
import com.tristanpenman.chordial.core.actors.{FindPredecessorAlgorithm, FindSuccessorAlgorithm, NotifyAlgorithm, StabilisationAlgorithm}
import com.tristanpenman.chordial.core.shared.NodeInfo

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

class Coordinator(nodeId: Long, requestTimeout: Timeout) extends Actor with ActorLogging {

  import Coordinator._

  private val getPredecessorTimeout = Timeout(2000.milliseconds)

  private val updatePredecessorTimeout = Timeout(2000.milliseconds)

  private def newNode(nodeId: Long, seedId: Long, seedRef: ActorRef) =
    context.actorOf(Node.props(nodeId, NodeInfo(seedId, seedRef)))

  private def newStabilisationAlgorithm() =
    context.actorOf(StabilisationAlgorithm.props())

  /**
   * Create an actor to execute the FindPredecessor algorithm and, when it finishes/fails, produce a response that
   * will be recognised by the original sender of the request
   *
   * This method passes in the ID and ActorRef of the current node as the initial candidate node, which means the
   * FindPredecessor algorithm will begin its search at the current node.
   */
  private def findPredecessor(queryId: Long, sender: ActorRef, algorithmTimeout: Timeout): Unit = {
    // The FindPredecessorAlgorithm actor will shutdown immediately after it sends a FindPredecessorAlgorithmOk or
    // FindPredecessorAlgorithmError message. However, if the future returned by the 'ask' request does not complete
    // within the timeout period, the actor must be shutdown manually to ensure that it does not run indefinitely.
    val findPredecessorAlgorithm = context.actorOf(FindPredecessorAlgorithm.props())
    findPredecessorAlgorithm.ask(FindPredecessorAlgorithmBegin(queryId, nodeId, self))(algorithmTimeout)
      .mapTo[FindPredecessorAlgorithmResponse]
      .map {
        case FindPredecessorAlgorithmOk(predecessorId, predecessorRef) =>
          FindPredecessorOk(queryId, predecessorId, predecessorRef)
        case FindPredecessorAlgorithmError(message) =>
          FindPredecessorError(queryId, message)
      }
      .recover {
        case exception =>
          context.stop(findPredecessorAlgorithm)
          FindPredecessorError(queryId, exception.getMessage)
      }
      .pipeTo(sender)
  }

  /**
   * Create an actor to execute the FindSuccessor algorithm and, when it finishes/fails, produce a response that
   * will be recognised by the original sender of the request.
   *
   * This method passes in the ActorRef of the current node as the search node, which means the operation will be
   * performed in the context of the current node.
   */
  private def findSuccessor(queryId: Long, sender: ActorRef, algorithmTimeout: Timeout): Unit = {
    // The FindSuccessorAlgorithm actor will shutdown immediately after it sends a FindSuccessorAlgorithmOk or
    // FindSuccessorAlgorithmError message. However, if the future returned by the 'ask' request does not complete
    // within the timeout period, the actor must be shutdown manually to ensure that it does not run indefinitely.
    val findSuccessorAlgorithm = context.actorOf(FindSuccessorAlgorithm.props())
    findSuccessorAlgorithm.ask(FindSuccessorAlgorithmBegin(queryId, self))(algorithmTimeout)
      .mapTo[FindSuccessorAlgorithmResponse]
      .map {
        case FindSuccessorAlgorithmOk(successorId, successorRef) =>
          FindSuccessorOk(queryId, successorId, successorRef)
        case FindSuccessorAlgorithmError(message) =>
          FindSuccessorError(queryId, message)
      }
      .recover {
        case exception =>
          context.stop(findSuccessorAlgorithm)
          FindSuccessorError(queryId, exception.getMessage)
      }
      .pipeTo(sender)
  }

  private def notify(nodeRef: ActorRef, candidate: NodeInfo, replyTo: ActorRef, timeout: Timeout) = {
    val notifyAlgorithm = context.actorOf(NotifyAlgorithm.props())
    notifyAlgorithm.ask(NotifyAlgorithmStart(NodeInfo(nodeId, self), candidate, nodeRef))(timeout)
      .mapTo[NotifyAlgorithmResponse]
      .map {
        case NotifyAlgorithmFinished(predecessorUpdated: Boolean) =>
          if (predecessorUpdated) {
            NotifyOk()
          } else {
            NotifyIgnored()
          }
      }
      .recover {
        case exception =>
          context.stop(notifyAlgorithm)
          NotifyError(exception.getMessage)
      }
      .pipeTo(replyTo)
  }

  private def stabilise(nodeRef: ActorRef, stabilisationAlgorithm: ActorRef, replyTo: ActorRef, timeout: Timeout) = {
    stabilisationAlgorithm.ask(StabilisationAlgorithmStart(NodeInfo(nodeId, self), nodeRef))(timeout)
      .mapTo[StabilisationAlgorithmStartResponse]
      .map {
        case StabilisationAlgorithmAlreadyRunning() =>
          StabiliseInProgress()
        case StabilisationAlgorithmFinished() =>
          StabiliseOk()
        case StabilisationAlgorithmFailed(message) =>
          StabiliseError(message)
      }
      .recover {
        case exception =>
          log.info(exception.getMessage)
          StabiliseError(exception.getMessage)
      }
      .pipeTo(replyTo)
  }

  def receiveWhileReady(nodeRef: ActorRef, stabilisationAlgorithm: ActorRef): Receive = {
    case m@ClosestPrecedingFinger(queryId: Long) =>
      nodeRef.ask(m)(requestTimeout).pipeTo(sender())

    case m@GetPredecessor() =>
      nodeRef.ask(m)(requestTimeout).pipeTo(sender())

    case m@GetSuccessor() =>
      nodeRef.ask(m)(requestTimeout).pipeTo(sender())

    case FindPredecessor(queryId) =>
      findPredecessor(queryId, sender(), requestTimeout)

    case FindSuccessor(queryId) =>
      findSuccessor(queryId, sender(), requestTimeout)

    case Join(seedId, seedRef) =>
      context.stop(nodeRef)
      context.stop(stabilisationAlgorithm)
      context.become(receiveWhileReady(newNode(nodeId, seedId, seedRef), newStabilisationAlgorithm()))
      sender() ! JoinOk()

    case Notify(candidateId, candidateRef) =>
      notify(nodeRef, NodeInfo(candidateId, candidateRef), sender(), requestTimeout)

    case Stabilise() =>
      stabilise(nodeRef, stabilisationAlgorithm, sender(), requestTimeout)
  }

  override def receive: Receive = receiveWhileReady(newNode(nodeId, nodeId, self), newStabilisationAlgorithm())
}

object Coordinator {

  class FindPredecessorResponse

  case class FindPredecessor(queryId: Long)

  case class FindPredecessorOk(queryId: Long, predecessorId: Long, predecessorRef: ActorRef)
    extends FindPredecessorResponse

  case class FindPredecessorError(queryId: Long, message: String) extends FindPredecessorResponse

  class FindSuccessorResponse

  case class FindSuccessor(queryId: Long)

  case class FindSuccessorOk(queryId: Long, successorId: Long, successorRef: ActorRef)
    extends FindSuccessorResponse

  case class FindSuccessorError(queryId: Long, message: String) extends FindSuccessorResponse

  case class Join(seedId: Long, seedRef: ActorRef)

  class JoinResponse

  case class JoinOk() extends JoinResponse

  case class JoinError(message: String) extends JoinResponse

  case class Notify(nodeId: Long, nodeRef: ActorRef)

  class NotifyResponse

  case class NotifyOk() extends NotifyResponse

  case class NotifyIgnored() extends NotifyResponse

  case class NotifyError(message: String) extends NotifyResponse

  case class Stabilise()

  class StabiliseResponse

  case class StabiliseInProgress() extends StabiliseResponse

  case class StabiliseOk() extends StabiliseResponse

  case class StabiliseError(message: String) extends StabiliseResponse

  def props(nodeId: Long, requestTimeout: Timeout): Props = Props(new Coordinator(nodeId, requestTimeout: Timeout))
}