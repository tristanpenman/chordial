package com.tristanpenman.chordial.daemon

import akka.actor.{Props, Actor, ActorLogging, ActorRef}
import akka.pattern.ask
import akka.util.Timeout
import com.tristanpenman.chordial.core.Coordinator
import com.tristanpenman.chordial.core.Coordinator._

import scala.annotation.tailrec
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.{Failure, Success, Random}

import scala.concurrent.ExecutionContext.Implicits.global

class Governor(val idModulus: Int) extends Actor with ActorLogging {

  import Governor._

  require(idModulus > 0, "idModulus must be a positive Int value")

  private val requestTimeout = Timeout(4000.milliseconds)
  private val checkPredecessorTimeout = Timeout(2500.milliseconds)
  private val livenessCheckDuration = 2000.milliseconds
  private val joinRequestTimeout = Timeout(2000.milliseconds)
  private val stabiliseTimeout = Timeout(1500.milliseconds)

  private def scheduleCheckPredecessor(nodeRef: ActorRef) =
    context.system.scheduler.schedule(300.milliseconds, 300.milliseconds) {
      nodeRef.ask(CheckPredecessor())(checkPredecessorTimeout)
        .mapTo[CheckPredecessorResponse]
        .onComplete {
          case util.Success(result) => result match {
            case CheckPredecessorOk() =>
              log.debug("CheckPredecessor (requested for {}) finished successfully", nodeRef.path)
            case CheckPredecessorInProgress() =>
              log.warning("CheckPredecessor (requested for {}) already in progress", nodeRef.path)
            case CheckPredecessorError(message) =>
              log.error("CheckPredecessor (requested for {}) finished with error: {}", nodeRef.path, message)
          }
          case util.Failure(exception) =>
            log.error("CheckPredecessor (requested for {}) failed with an exception: {}", nodeRef.path, exception)
        }
    }

  private def scheduleStabilisation(nodeRef: ActorRef) =
    context.system.scheduler.schedule(200.milliseconds, 200.milliseconds) {
      nodeRef.ask(Stabilise())(stabiliseTimeout)
        .mapTo[StabiliseResponse]
        .onComplete {
          case util.Success(result) => result match {
            case StabiliseOk() =>
              log.debug("Stabilisation (requested for {}) finished successfully", nodeRef.path)
            case StabiliseInProgress() =>
              log.warning("Stabilisation (requested for {}) already in progress", nodeRef.path)
            case StabiliseError(message) =>
              log.error("Stabilisation (requested for {}) finished with error: {}", nodeRef.path, message)
          }
          case util.Failure(exception) =>
            log.error("Stabilisation (requested for {}) failed with an exception: {}", nodeRef.path, exception)
        }
    }

  private def createNode(nodeId: Long): ActorRef = {
    val nodeRef = context.system.actorOf(Coordinator.props(nodeId, requestTimeout, livenessCheckDuration,
      context.system.eventStream))
    scheduleCheckPredecessor(nodeRef)
    scheduleStabilisation(nodeRef)
    nodeRef
  }

  @tailrec
  private def generateUniqueId(nodeIds: Set[Long]): Long = {
    val id = Random.nextInt(idModulus)
    if (!nodeIds.contains(id)) {
      id
    } else {
      generateUniqueId(nodeIds)
    }
  }

  private def receiveWithNodes(nodes: Map[Long, ActorRef]): Receive = {
    case CreateNode() =>
      if (nodes.size < idModulus) {
        val nodeId = generateUniqueId(nodes.keySet)
        val nodeRef = createNode(nodeId)
        context.become(receiveWithNodes(nodes + (nodeId -> nodeRef)))
        sender() ! CreateNodeOk(nodeId, nodeRef)
      } else {
        sender() ! CreateNodeError(s"Maximum of $idModulus Chord nodes already running")
      }

    case CreateNodeWithSeed(seedId) =>
      nodes.get(seedId) match {
        case Some(seedRef) =>
          val nodeId = generateUniqueId(nodes.keySet)
          val nodeRef = createNode(nodeId)

          val joinRequest = nodeRef.ask(Join(seedId, seedRef))(joinRequestTimeout)
            .mapTo[JoinResponse]
            .map {
              case JoinOk() => Success(())
              case JoinError(message) => throw new Exception(message)
            }
            .recover {
              case ex => Failure(ex)
            }

          Await.result(joinRequest, Duration.Inf) match {
            case Success(()) =>
              context.become(receiveWithNodes(nodes + (nodeId -> nodeRef)))
              sender() ! CreateNodeWithSeedOk(nodeId, nodeRef)
            case Failure(ex) =>
              context.stop(nodeRef)
              sender() ! CreateNodeWithSeedError(ex.getMessage)
          }

        case None =>
          sender() ! CreateNodeWithSeedError(s"Node with ID $seedId does not exist")
      }
  }

  override def receive: Receive = receiveWithNodes(Map.empty)
}

object Governor {

  sealed trait Request

  sealed trait Response

  case class CreateNode() extends Request

  sealed trait CreateNodeResponse extends Response

  case class CreateNodeOk(nodeId: Long, nodeRef: ActorRef) extends CreateNodeResponse

  case class CreateNodeError(message: String) extends CreateNodeResponse

  case class CreateNodeWithSeed(seedId: Long) extends Request

  sealed trait CreateNodeWithSeedResponse extends Response

  case class CreateNodeWithSeedOk(nodeId: Long, nodeRef: ActorRef) extends CreateNodeWithSeedResponse

  case class CreateNodeWithSeedError(message: String) extends CreateNodeWithSeedResponse

  def props(idModulus: Int): Props = Props(new Governor(idModulus))

}