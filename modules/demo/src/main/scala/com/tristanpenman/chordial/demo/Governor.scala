package com.tristanpenman.chordial.demo

import java.net.InetSocketAddress

import akka.actor._
import akka.pattern.{ask, pipe}
import akka.util.Timeout
import com.tristanpenman.chordial.core.Event.NodeShuttingDown
import com.tristanpenman.chordial.core.Node._
import com.tristanpenman.chordial.core.Pointers.{GetSuccessor, GetSuccessorOk, GetSuccessorResponse}
import com.tristanpenman.chordial.core.Router.{Start, StartFailed, StartOk}
import com.tristanpenman.chordial.core.{Node, Router}

import scala.annotation.tailrec
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.{Failure, Random, Success}

final class Governor(val keyspaceBits: Int) extends Actor with ActorLogging with Stash {
  import Governor._
  import context.dispatcher

  private val router = context.system.actorOf(Router.props())

  router ! Start("0.0.0.0", 0)

  require(keyspaceBits > 0, "keyspaceBits must be a positive Int value")

  private val idModulus = 1 << keyspaceBits

  // How long to wait when making requests that may be routed to other nodes
  private val externalRequestTimeout = Timeout(500.milliseconds)

  // How long Node should wait until an algorithm is considered to have timed out. This should be significantly
  // longer than the external request timeout, as some algorithms will make multiple external requests before
  // running to completion
  private val algorithmTimeout = Timeout(5000.milliseconds)

  private val joinRequestTimeout = Timeout(2000.milliseconds)
  private val getSuccessorRequestTimeout = Timeout(2000.milliseconds)

  private def createNode(nodeId: Long, nodeAddr: InetSocketAddress): ActorRef =
    context.system.actorOf(
      Node.props(
        nodeId,
        nodeAddr,
        keyspaceBits,
        algorithmTimeout,
        externalRequestTimeout,
        context.system.eventStream,
        router
      )
    )

  @tailrec
  private def generateUniqueId(nodeIds: Set[Long]): Long = {
    val id = Random.nextInt(idModulus)
    if (nodeIds.contains(id)) generateUniqueId(nodeIds) else id
  }

  private def receiveWithNodes(nodeAddr: InetSocketAddress,
                               nodes: Map[Long, ActorRef],
                               terminatedNodes: Set[Long]): Receive = {
    case CreateNode =>
      if (nodes.size < idModulus) {
        val nodeId = generateUniqueId(nodes.keySet ++ terminatedNodes)
        val nodeRef = createNode(nodeId, nodeAddr)
        context.become(receiveWithNodes(nodeAddr, nodes + (nodeId -> nodeRef), terminatedNodes))
        sender() ! CreateNodeOk(nodeId, nodeRef)
      } else {
        sender() ! CreateNodeInvalidRequest(s"Maximum of $idModulus Chord nodes already created")
      }

    case CreateNodeWithSeed(seedId, seedAddr) =>
      nodes.get(seedId) match {
        case Some(seedRef) =>
          if (nodes.size < idModulus) {
            val nodeId = generateUniqueId(nodes.keySet ++ terminatedNodes)
            val nodeRef = createNode(nodeId, nodeAddr)
            val joinRequest = nodeRef
              .ask(Join(seedId, seedAddr, seedRef))(joinRequestTimeout)
              .mapTo[JoinResponse]
              .map {
                case JoinOk             => Success(())
                case JoinError(message) => throw new Exception(message)
              }
              .recover {
                case ex => Failure(ex)
              }

            Await.result(joinRequest, Duration.Inf) match {
              case Success(()) =>
                context.become(receiveWithNodes(nodeAddr, nodes + (nodeId -> nodeRef), terminatedNodes))
                sender() ! CreateNodeWithSeedOk(nodeId, nodeRef)
              case Failure(ex) =>
                context.stop(nodeRef)
                sender() ! CreateNodeWithSeedInternalError(ex.getMessage)
            }
          } else {
            sender() ! CreateNodeWithSeedInvalidRequest(s"Maximum of $idModulus Chord nodes already created")
          }

        case None =>
          sender() ! CreateNodeWithSeedInvalidRequest(s"Node with ID $seedId does not exist")
      }

    case GetNodeIdSet =>
      sender() ! GetNodeIdSetOk(nodes.keySet ++ terminatedNodes)

    case GetNodeState(nodeId: Long) =>
      if (nodes.contains(nodeId)) {
        sender() ! GetNodeStateOk(true)
      } else if (terminatedNodes.contains(nodeId)) {
        sender() ! GetNodeStateOk(false)
      } else {
        sender() ! GetNodeStateError(s"Node with ID $nodeId does not exist")
      }

    case GetNodeSuccessorId(nodeId: Long) =>
      nodes.get(nodeId) match {
        case Some(nodeRef) =>
          nodeRef
            .ask(GetSuccessor)(getSuccessorRequestTimeout)
            .mapTo[GetSuccessorResponse]
            .map {
              case GetSuccessorOk(successor) =>
                GetNodeSuccessorIdOk(successor.id)
            }
            .recover {
              case ex => GetNodeSuccessorIdError(ex.getMessage)
            }
            .pipeTo(sender())
        case None =>
          if (terminatedNodes.contains(nodeId)) {
            sender() ! GetNodeSuccessorIdInvalidRequest(s"Node with ID $nodeId is no longer active")
          } else {
            sender() ! GetNodeSuccessorIdInvalidRequest(s"Node with ID $nodeId does not exist")
          }
      }

    case TerminateNode(nodeId: Long) =>
      nodes.get(nodeId) match {
        case Some(nodeRef) =>
          context.stop(nodeRef)
          context.become(receiveWithNodes(nodeAddr, nodes - nodeId, terminatedNodes + nodeId))
          sender() ! TerminateNodeResponseOk
          context.system.eventStream.publish(NodeShuttingDown(nodeId))

        case None =>
          sender() ! TerminateNodeResponseError(s"Node with ID $nodeId does not exist")
      }
  }

  override def receive: Receive = {
    case StartOk(localAddress) =>
      context.become(receiveWithNodes(localAddress, Map.empty, Set.empty))
      unstashAll()

    case StartFailed(reason) =>
      throw new Exception(s"Failed to start Router: ${reason}")

    case _ =>
      stash()
  }
}

object Governor {

  sealed trait Request

  sealed trait Response

  case object CreateNode extends Request

  sealed trait CreateNodeResponse extends Response

  final case class CreateNodeOk(nodeId: Long, nodeRef: ActorRef) extends CreateNodeResponse

  final case class CreateNodeInternalError(message: String) extends CreateNodeResponse

  final case class CreateNodeInvalidRequest(message: String) extends CreateNodeResponse

  final case class CreateNodeWithSeed(seedId: Long, seedAddr: InetSocketAddress) extends Request

  sealed trait CreateNodeWithSeedResponse extends Response

  final case class CreateNodeWithSeedOk(nodeId: Long, nodeRef: ActorRef) extends CreateNodeWithSeedResponse

  final case class CreateNodeWithSeedInternalError(message: String) extends CreateNodeWithSeedResponse

  final case class CreateNodeWithSeedInvalidRequest(message: String) extends CreateNodeWithSeedResponse

  case object GetNodeIdSet extends Request

  sealed trait GetNodeIdSetResponse extends Response

  final case class GetNodeIdSetOk(nodeIds: Set[Long]) extends GetNodeIdSetResponse

  final case class GetNodeState(nodeId: Long) extends Request

  sealed trait GetNodeStateResponse extends Response

  final case class GetNodeStateOk(active: Boolean) extends GetNodeStateResponse

  final case class GetNodeStateError(message: String) extends GetNodeStateResponse

  final case class GetNodeStateInvalidRequest(message: String) extends GetNodeStateResponse

  final case class GetNodeSuccessorId(nodeId: Long) extends Request

  sealed trait GetNodeSuccessorIdResponse extends Response

  final case class GetNodeSuccessorIdOk(successorId: Long) extends GetNodeSuccessorIdResponse

  final case class GetNodeSuccessorIdError(message: String) extends GetNodeSuccessorIdResponse

  final case class GetNodeSuccessorIdInvalidRequest(message: String) extends GetNodeSuccessorIdResponse

  final case class TerminateNode(nodeId: Long) extends Request

  sealed trait TerminateNodeResponse extends Response

  case object TerminateNodeResponseOk extends TerminateNodeResponse

  final case class TerminateNodeResponseError(message: String) extends TerminateNodeResponse

  def props(keyspaceBits: Int): Props = Props(new Governor(keyspaceBits))

}
