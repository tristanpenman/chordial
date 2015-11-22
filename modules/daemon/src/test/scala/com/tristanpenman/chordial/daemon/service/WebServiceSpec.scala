package com.tristanpenman.chordial.daemon.service

import akka.actor.{ActorSystem, Actor, ActorRef}
import akka.testkit.TestActorRef
import com.tristanpenman.chordial.core.Node._
import com.tristanpenman.chordial.daemon.WebService
import org.scalatest.{FlatSpec, ShouldMatchers}
import spray.testkit.ScalatestRouteTest

class WebServiceSpec extends FlatSpec with ShouldMatchers with WebService with ScalatestRouteTest {
  def actorRefFactory: ActorSystem = system

  override protected def governor: ActorRef = TestActorRef(new Actor {
    def receive: Receive = {
      case GetId() =>
        sender() ! GetIdOk(0L)
      case GetPredecessor() =>
        sender() ! GetPredecessorOk(-1L, self)
      case GetSuccessor() =>
        sender() ! GetSuccessorOk(1L, self)
    }
  })

  "The service" should "return its own ID for GET requests to the / endpoint" in {
    Get() ~> routes ~> check {
      responseAs[String] should be("0")
    }
  }

  "The service" should "return the ID of the predecessor for GET requests to the /predecessor endpoint" in {
    Get("/predecessor") ~> routes ~> check {
      responseAs[String] should be("-1")
    }
  }

  "The service" should "return the ID of the successor for GET requests to the /successor endpoint" in {
    Get("/successor") ~> routes ~> check {
      responseAs[String] should be("1")
    }
  }
}
