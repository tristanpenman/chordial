package com.tristanpenman.chordial.daemon.service

import akka.actor.ActorRef
import akka.pattern.ask
import akka.util.Timeout
import com.tristanpenman.chordial.daemon.Governor._
import spray.http.MediaTypes._
import spray.http.StatusCodes
import spray.httpx.marshalling.ToResponseMarshallable
import spray.json.DefaultJsonProtocol._
import spray.json._
import spray.routing._

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContextExecutor, Future}

trait Service extends HttpService {
  implicit def ec: ExecutionContextExecutor = actorRefFactory.dispatcher

  implicit val timeout: Timeout = 3.seconds

  protected def governor: ActorRef

  private type NodeAttributes = Map[String, Long]

  private type Nodes = Iterable[NodeAttributes]

  private def nodeAttributeMap(nodeId: Long, successorId: Long): NodeAttributes =
    Map("nodeId" -> nodeId, "successorId" -> successorId)

  private def getNodeAttributes(nodeId: Long): Future[NodeAttributes] = governor.ask(GetNodeSuccessorId(nodeId))
    .mapTo[GetNodeSuccessorIdResponse]
    .map {
      case GetNodeSuccessorIdOk(successorId) =>
        nodeAttributeMap(nodeId, successorId)
      case GetNodeSuccessorIdError(message) =>
        throw new Exception(message)
    }

  private def getNodes: Future[Nodes] = governor.ask(GetNodeIdSet())
    .mapTo[GetNodeIdSetResponse]
    .flatMap {
      case GetNodeIdSetOk(nodeIdSet) =>
        Future.sequence(
          nodeIdSet.map { case nodeId => getNodeAttributes(nodeId) }
        )
    }

  private def terminateNode(nodeId: Long): Future[Unit] = governor.ask(TerminateNode(nodeId))
    .mapTo[TerminateNodeResponse]
    .map {
      case TerminateNodeResponseOk() =>
      case TerminateNodeResponseError(message: String) =>
        throw new Exception(message)
    }

  val routes = pathPrefix("nodes") {
    pathEnd {
      get {
        respondWithMediaType(`application/json`) {
          complete {
            ToResponseMarshallable.isMarshallable(getNodes.map {
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
                          nodeAttributeMap(nodeId, seedId.toLong).toJson.compactPrint
                        case CreateNodeWithSeedError(message) =>
                          throw new Exception(message)
                      }
                  case None =>
                    governor.ask(CreateNode())
                      .mapTo[CreateNodeResponse]
                      .map {
                        case CreateNodeOk(nodeId, nodeRef) =>
                          nodeAttributeMap(nodeId, nodeId).toJson.compactPrint
                        case CreateNodeError(message) =>
                          throw new Exception(message)
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
            ToResponseMarshallable.isMarshallable(getNodeAttributes(nodeId).map {
              _.toJson.compactPrint
            })
          }
        }
      } ~ delete {
        complete {
          ToResponseMarshallable.isMarshallable(
            terminateNode(nodeId).map { _ => StatusCodes.OK }
          )
        }
      }
    }
  }
}
