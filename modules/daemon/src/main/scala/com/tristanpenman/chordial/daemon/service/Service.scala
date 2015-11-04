package com.tristanpenman.chordial.daemon.service

import akka.actor.ActorRef
import akka.pattern.ask
import akka.util.Timeout
import spray.http.MediaTypes._
import spray.httpx.marshalling.ToResponseMarshallable
import spray.routing._

import com.tristanpenman.chordial.core.NodeProtocol._

import scala.concurrent.duration._

trait Service extends HttpService {
  implicit def ec = actorRefFactory.dispatcher

  implicit val timeout: Timeout = 3.seconds

  protected def id: Long
  protected def ref: ActorRef

  val routes = path("") {
    get {
      respondWithMediaType(`text/plain`) {
        complete {
          ToResponseMarshallable.isMarshallable(id.toString)
        }
      }
    }
  } ~ path("successor") {
    get {
      respondWithMediaType(`text/plain`) {
        complete {
          ToResponseMarshallable.isMarshallable(
            ref.ask(GetSuccessor())
              .mapTo[GetSuccessorResponse]
              .map {
                case GetSuccessorOk(id, _) => id.toString
                case _ => "unknown"
              }
          )
        }
      }
    }
  } ~ path("predecessor") {
    get {
      respondWithMediaType(`text/plain`) {
        complete {
          ToResponseMarshallable.isMarshallable(
            ref.ask(GetPredecessor())
              .mapTo[GetPredecessorResponse]
              .map {
                case GetPredecessorOk(id, _) => id.toString
                case _ => "unknown"
              }
          )
        }
      }
    }
  }
}
