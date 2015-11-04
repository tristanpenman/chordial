package com.tristanpenman.chordial.daemon.service

import akka.actor.{Actor, ActorRef}
import akka.testkit.TestActorRef
import com.tristanpenman.chordial.core.NodeProtocol.{GetSuccessorOk, GetSuccessor}
import org.scalatest.{FlatSpec, ShouldMatchers}
import spray.testkit.ScalatestRouteTest

class ServiceSpec extends FlatSpec with ShouldMatchers with Service with ScalatestRouteTest {
  def actorRefFactory = system

  override protected def id: Long = 0

  override protected def ref: ActorRef = TestActorRef(new Actor {
    def receive = {
      case GetSuccessor() =>
        sender() ! GetSuccessorOk(1, self)
    }
  })

  "The service" should "return its own ID for GET requests to the / endpoint" in {
    Get() ~> routes ~> check {
      responseAs[String] should be("0")
    }
  }

  "The service" should "return the ID of the successor for GET requests to the /successor endpoint" in {
    Get("/successor") ~> routes ~> check {
      responseAs[String] should be("1")
    }
  }
}
