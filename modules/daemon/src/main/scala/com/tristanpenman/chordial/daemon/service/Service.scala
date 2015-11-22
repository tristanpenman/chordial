package com.tristanpenman.chordial.daemon.service

import akka.actor.ActorRef
import akka.pattern.ask
import akka.util.Timeout
import com.tristanpenman.chordial.daemon.Governor._
import spray.http.MediaTypes._
import spray.httpx.marshalling.ToResponseMarshallable
import spray.routing._

import scala.concurrent.ExecutionContextExecutor
import scala.concurrent.duration._

trait Service extends HttpService {
  implicit def ec: ExecutionContextExecutor = actorRefFactory.dispatcher

  implicit val timeout: Timeout = 3.seconds

  protected def governor: ActorRef

  val routes = path("") {
    post {
      parameters('seed_id.?) { (maybeSeedId) =>
        respondWithMediaType(`text/plain`) {
          complete {
            ToResponseMarshallable.isMarshallable(
              maybeSeedId match {
                case Some(seedId) =>
                  governor.ask(CreateNodeWithSeed(seedId.toLong))
                    .mapTo[CreateNodeWithSeedResponse]
                    .map {
                      case CreateNodeWithSeedOk(nodeId, nodeRef) => nodeId.toString
                      case CreateNodeWithSeedError(message) => message
                    }
                    .recover {
                      case ex => ex.getMessage
                    }
                case None =>
                  governor.ask(CreateNode())
                    .mapTo[CreateNodeResponse]
                    .map {
                      case CreateNodeOk(nodeId, nodeRef) => nodeId.toString
                      case CreateNodeError(message) => message
                    }
                    .recover {
                      case ex => ex.getMessage
                    }
              }
            )
          }
        }
      }
    }
  }
}
