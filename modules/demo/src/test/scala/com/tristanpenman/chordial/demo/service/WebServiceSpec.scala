package com.tristanpenman.chordial.demo.service

import akka.actor.{Actor, ActorRef, ActorSystem}
import akka.testkit.TestActorRef
import com.tristanpenman.chordial.demo.Governor._
import com.tristanpenman.chordial.demo.WebService
import org.scalatest.{ShouldMatchers, WordSpec}
import spray.json._
import spray.testkit.ScalatestRouteTest

class WebServiceSpec extends WordSpec with ShouldMatchers with WebService with ScalatestRouteTest {

  import WebService._

  def actorRefFactory: ActorSystem = system

  private def dummyActor: ActorRef = TestActorRef(new Actor {
    def receive: Receive = {
      case message =>
        fail(s"Dummy actor should not receive any messages, but just received: ${message}")
    }
  })

  override protected def governor: ActorRef = TestActorRef(new Actor {
    def receive: Receive = {
      case CreateNode() =>
        sender() ! CreateNodeOk(1L, dummyActor)
      case CreateNodeWithSeed(seedId) =>
        sender() ! CreateNodeWithSeedOk(2L, dummyActor)
      case GetNodeIdSet() =>
        sender() ! GetNodeIdSetOk(Set.empty)
    }
  })

  "The web service" when {
    "backed by a Governor with no nodes" should {
      "respond to a GET request on the /nodes endpoint with an empty JSON array" in {
        Get("/nodes") ~> routes ~> check {
          val jsonAst = responseAs[String].parseJson
          val jsonAsNodeAttrArray = jsonAst.convertTo[Iterable[NodeAttributes]]
          assert(jsonAsNodeAttrArray.isEmpty)
        }
      }
      "respond to a POST request on the /nodes endpoint with the correct JSON object" in {
        Post("/nodes") ~> routes ~> check {
          val jsonAst = responseAs[String].parseJson
          val jsonAsNodeAttr = jsonAst.convertTo[NodeAttributes]
          assert(jsonAsNodeAttr.nodeId == 1L)
          assert(jsonAsNodeAttr.successorId.contains(1L))
          assert(jsonAsNodeAttr.active)
        }
      }
      "respond to a POST request on the /nodes endpoint (including a seed ID) with the correct JSON object" in {
        Post("/nodes?seed_id=1") ~> routes ~> check {
          val jsonAst = responseAs[String].parseJson
          val jsonAsNodeAttr = jsonAst.convertTo[NodeAttributes]
          assert(jsonAsNodeAttr.nodeId == 2L)
          assert(jsonAsNodeAttr.successorId.contains(1L))
          assert(jsonAsNodeAttr.active)
        }
      }
    }
  }
}
