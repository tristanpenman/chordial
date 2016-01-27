package com.tristanpenman.chordial.demo

import akka.actor.ActorRef
import akka.pattern.ask
import akka.util.Timeout
import com.tristanpenman.chordial.demo.Governor._
import spray.http.MediaTypes._
import spray.http.StatusCodes
import spray.httpx.marshalling.ToResponseMarshallable
import spray.json._
import spray.routing._

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContextExecutor, Future}

object WebService extends DefaultJsonProtocol {
  case class NodeAttributes(nodeId: Long, successorId: Option[Long], active: Boolean)

  implicit val nodeAttributeFormat = jsonFormat3(NodeAttributes)
}

trait WebService extends HttpService {

  import WebService._

  implicit def ec: ExecutionContextExecutor = actorRefFactory.dispatcher

  implicit val timeout: Timeout = 3.seconds

  private def getNodeAttributes(governor: ActorRef, nodeId: Long): Future[NodeAttributes] = governor.ask(GetNodeState(nodeId))
    .mapTo[GetNodeStateResponse]
    .map {
      case GetNodeStateOk(active) =>
        active
      case GetNodeStateError(message) =>
        throw new Exception(message)
    }
    .flatMap { case active =>
      if (active) {
        governor.ask(GetNodeSuccessorId(nodeId))
          .mapTo[GetNodeSuccessorIdResponse]
          .map {
            case GetNodeSuccessorIdOk(successorId) =>
              NodeAttributes(nodeId, Some(successorId), active = true)
            case GetNodeSuccessorIdError(message) =>
              throw new Exception(message)
          }
      } else {
        Future {
          NodeAttributes(nodeId, None, active = false)
        }
      }
    }

  private def getNodes(governor: ActorRef): Future[Iterable[NodeAttributes]] = governor.ask(GetNodeIdSet())
    .mapTo[GetNodeIdSetResponse]
    .flatMap {
      case GetNodeIdSetOk(nodeIdSet) =>
        Future.sequence(
          nodeIdSet.map { case nodeId => getNodeAttributes(governor, nodeId) }
        )
    }

  private def terminateNode(governor: ActorRef, nodeId: Long): Future[Unit] = governor.ask(TerminateNode(nodeId))
    .mapTo[TerminateNodeResponse]
    .map {
      case TerminateNodeResponseOk() =>
      case TerminateNodeResponseError(message: String) =>
        throw new Exception(message)
    }

  protected def routes(governor: ActorRef) = pathPrefix("nodes") {
    pathEnd {
      get {
        respondWithMediaType(`application/json`) {
          complete {
            ToResponseMarshallable.isMarshallable(getNodes(governor).map {
              _.toJson.compactPrint
            })
          }
        }
      } ~ post {
        parameters('seed_id.?) {
          (maybeSeedId) => respondWithMediaType(`application/json`) {
            complete {
              ToResponseMarshallable.isMarshallable(
                maybeSeedId match {
                  case Some(seedId) =>
                    governor.ask(CreateNodeWithSeed(seedId.toLong))
                      .mapTo[CreateNodeWithSeedResponse]
                      .map {
                        case CreateNodeWithSeedOk(nodeId, nodeRef) =>
                          NodeAttributes(nodeId, Some(seedId.toLong), active = true).toJson.compactPrint
                        case CreateNodeWithSeedInternalError(message) =>
                          throw new Exception(message)
                        case CreateNodeWithSeedInvalidRequest(message) =>
                          respondWithStatus(StatusCodes.BadRequest)
                          message
                      }
                  case None =>
                    governor.ask(CreateNode())
                      .mapTo[CreateNodeResponse]
                      .map {
                        case CreateNodeOk(nodeId, nodeRef) =>
                          NodeAttributes(nodeId, Some(nodeId), active = true).toJson.compactPrint
                        case CreateNodeInternalError(message) =>
                          throw new Exception(message)
                        case CreateNodeInvalidRequest(message) =>
                          respondWithStatus(StatusCodes.BadRequest)
                          message
                      }
                }
              )
            }
          }
        }
      }
    } ~ path(IntNumber) {
      nodeId => get {
        respondWithMediaType(`application/json`) {
          complete {
            ToResponseMarshallable.isMarshallable(getNodeAttributes(governor, nodeId).map {
              _.toJson.compactPrint
            })
          }
        }
      } ~ delete {
        complete {
          ToResponseMarshallable.isMarshallable(
            terminateNode(governor, nodeId).map { _ => StatusCodes.OK }
          )
        }
      }
    }
  }
}
