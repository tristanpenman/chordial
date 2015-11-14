package com.tristanpenman.chordial.core

import akka.actor._
import akka.pattern.{ask, pipe}
import akka.util.Timeout
import com.tristanpenman.chordial.core.NodeProtocol._
import com.tristanpenman.chordial.core.actors.StabilisationAlgorithm
import com.tristanpenman.chordial.core.actors.StabilisationAlgorithm.{StabilisationAlgorithmAlreadyRunning, StabilisationAlgorithmFinished, StabilisationAlgorithmStart}
import com.tristanpenman.chordial.core.shared.NodeInfo

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

class Coordinator(nodeId: Long, seed: Option[NodeInfo], restartTimeout: Timeout, stabilisationAlgorithmTimeout: Timeout,
                  updateSuccessorTimeout: Timeout) extends Actor {

  import Coordinator._

  private def newNode(nodeId: Long, seed: Option[NodeInfo]) =
    context.actorOf(Node.props(nodeId, seed))

  private def newStabilisationAlgorithm() =
    context.actorOf(StabilisationAlgorithm.props())

  private def stabiliseThenUpdateSuccessor(nodeRef: ActorRef, stabilisationAlgorithm: ActorRef) =
    stabilisationAlgorithm.ask(StabilisationAlgorithmStart(NodeInfo(nodeId, nodeRef)))(stabilisationAlgorithmTimeout)
      .flatMap {
        case StabilisationAlgorithmFinished(newSuccessor) =>
          nodeRef.ask(UpdateSuccessor(newSuccessor.id, newSuccessor.ref))(updateSuccessorTimeout).map {
            case UpdateSuccessorOk => false
          }
        case StabilisationAlgorithmAlreadyRunning() =>
          Future {
            true
          }
      }

  def receiveWhileReady(nodeRef: ActorRef, stabilisationAlgorithm: ActorRef): Receive = {
    case Join(seedId, seedRef) =>
      context.stop(nodeRef)
      context.stop(stabilisationAlgorithm)
      context.become(receiveWhileReady(newNode(nodeId, Some(NodeInfo(seedId, seedRef))), newStabilisationAlgorithm()))
      sender() ! JoinOk()

    case Stabilise() =>
      stabiliseThenUpdateSuccessor(nodeRef, stabilisationAlgorithm)
        .map {
          case true =>
            StabiliseInProgress()
          case false =>
            StabiliseOk()
        }
        .recover {
          case exception =>
            StabiliseError(exception.getMessage)
        }
        .pipeTo(sender())

  }

  override def receive: Receive = receiveWhileReady(newNode(nodeId, seed), newStabilisationAlgorithm())
}

object Coordinator {

  case class Join(seedId: Long, seedRef: ActorRef)

  class JoinResponse

  case class JoinOk() extends JoinResponse

  case class JoinError(message: String) extends JoinResponse

  case class Stabilise()

  class StabiliseResponse

  case class StabiliseInProgress() extends StabiliseResponse

  case class StabiliseOk() extends StabiliseResponse

  case class StabiliseError(message: String) extends StabiliseResponse

  def props(nodeId: Long, seed: Option[NodeInfo], restartTimeout: Timeout, stabilisationAlgorithmTimeout: Timeout,
            updateSuccessorTimeout: Timeout): Props =
    Props(new Coordinator(nodeId, seed, restartTimeout, stabilisationAlgorithmTimeout, updateSuccessorTimeout))
}