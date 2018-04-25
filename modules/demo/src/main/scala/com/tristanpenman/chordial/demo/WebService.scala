package com.tristanpenman.chordial.demo

import akka.actor.ActorRef
import akka.http.scaladsl.model.StatusCodes._
import akka.http.scaladsl.server.Directives._
import akka.pattern.ask
import akka.util.Timeout
import com.tristanpenman.chordial.demo.Governor._
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._
import spray.json._

object WebService extends DefaultJsonProtocol {
  final case class NodeAttributes(nodeId: Long, successorId: Option[Long], active: Boolean)

  implicit val nodeAttributeFormat = jsonFormat3(NodeAttributes)
}

trait WebService {

  import WebService._

  implicit def ec: ExecutionContext

  implicit val timeout: Timeout = 3.seconds

  private def messageForInternalServerError =
    "The request failed due to an internal server error. Details will be available in the server logs."

  private def getNodeAttributes(governor: ActorRef, nodeId: Long): Future[NodeAttributes] =
    governor
      .ask(GetNodeState(nodeId))
      .mapTo[GetNodeStateResponse]
      .map {
        case GetNodeStateOk(active) =>
          active
        case GetNodeStateError(message) =>
          throw new Exception(message)
        case GetNodeStateInvalidRequest(message) =>
          throw new Exception(s"Governor rejected request for node state ($message)")
      }
      .flatMap {
        case active =>
          if (active) {
            governor
              .ask(GetNodeSuccessorId(nodeId))
              .mapTo[GetNodeSuccessorIdResponse]
              .map {
                case GetNodeSuccessorIdOk(successorId) =>
                  NodeAttributes(nodeId, Some(successorId), active = true)
                case GetNodeSuccessorIdError(message) =>
                  throw new Exception(message)
                case GetNodeSuccessorIdInvalidRequest(message) =>
                  throw new Exception(s"Governor rejected request for node successor ID ($message)")
              }
          } else {
            Future {
              NodeAttributes(nodeId, None, active = false)
            }
          }
      }

  private def getNodes(governor: ActorRef): Future[Iterable[NodeAttributes]] =
    governor
      .ask(GetNodeIdSet)
      .mapTo[GetNodeIdSetResponse]
      .flatMap {
        case GetNodeIdSetOk(nodeIdSet) =>
          Future.sequence(nodeIdSet.map { case nodeId => getNodeAttributes(governor, nodeId) })
      }

  private def terminateNode(governor: ActorRef, nodeId: Long): Future[Unit] =
    governor
      .ask(TerminateNode(nodeId))
      .mapTo[TerminateNodeResponse]
      .map {
        case TerminateNodeResponseOk =>
        case TerminateNodeResponseError(message: String) =>
          throw new Exception(message)
      }

  protected def routes(governor: ActorRef) = pathPrefix("nodes") {
    pathEndOrSingleSlash {
      get {
        val future = getNodes(governor)
        onComplete(future) {
          case util.Success(result) =>
            complete(result.toJson.compactPrint)
          case util.Failure(exception) =>
            complete(InternalServerError -> messageForInternalServerError)
        }
      } ~ post {
        parameters('seed_id.?) { (maybeSeedId) =>
          maybeSeedId match {
            case Some(seedId) =>
              val future = governor
                .ask(CreateNodeWithSeed(seedId.toLong))
                .mapTo[CreateNodeWithSeedResponse]
              onSuccess(future) {
                case CreateNodeWithSeedOk(nodeId, nodeRef) =>
                  complete(NodeAttributes(nodeId, Some(seedId.toLong), active = true).toJson.compactPrint)
                case CreateNodeWithSeedInternalError(_) =>
                  complete(InternalServerError -> messageForInternalServerError)
                case CreateNodeWithSeedInvalidRequest(message) =>
                  complete(BadRequest -> message)
              }
            case None =>
              val future =
                governor.ask(CreateNode).mapTo[CreateNodeResponse]
              onSuccess(future) {
                case CreateNodeOk(nodeId, nodeRef) =>
                  complete(NodeAttributes(nodeId, Some(nodeId), active = true).toJson.compactPrint)
                case CreateNodeInternalError(_) =>
                  complete(InternalServerError -> messageForInternalServerError)
                case CreateNodeInvalidRequest(message) =>
                  complete(BadRequest -> message)
              }
          }
        }
      }
    } ~ path(IntNumber) { nodeId =>
      get {
        val future = governor.ask(GetNodeIdSet).mapTo[GetNodeIdSetResponse]
        onComplete(future) {
          case util.Success(GetNodeIdSetOk(nodeIdSet)) =>
            if (nodeIdSet.contains(nodeId)) {
              complete {
                getNodeAttributes(governor, nodeId).map {
                  _.toJson.compactPrint
                }
              }
            } else {
              complete(BadRequest -> s"Node with ID $nodeId does not exist")
            }
          case _ =>
            complete(InternalServerError -> messageForInternalServerError)
        }
      } ~ delete {
        complete {
          terminateNode(governor, nodeId).map { _ =>
            OK
          }
        }
      }
    }
  }
}
