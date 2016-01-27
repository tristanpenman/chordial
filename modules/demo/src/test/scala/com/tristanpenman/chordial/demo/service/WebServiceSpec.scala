package com.tristanpenman.chordial.demo.service

import akka.actor.{Actor, ActorRef, ActorSystem}
import akka.testkit.TestActorRef
import com.tristanpenman.chordial.demo.Governor.{GetNodeIdSet, GetNodeIdSetOk}
import com.tristanpenman.chordial.demo.WebService
import org.scalatest.{ShouldMatchers, WordSpec}
import spray.json._
import spray.testkit.ScalatestRouteTest

class WebServiceSpec extends WordSpec with ShouldMatchers with WebService with ScalatestRouteTest {

  import WebService._

  def actorRefFactory: ActorSystem = system

  override protected def governor: ActorRef = TestActorRef(new Actor {
    def receive: Receive = {
      case GetNodeIdSet() =>
        sender() ! GetNodeIdSetOk(Set.empty)
    }
  })

  "The web service" when {
    "backed by a Governor with no nodes" should {
      "respond to GET requests on the /nodes endpoint with an empty JSON array" in {
        Get("/nodes") ~> routes ~> check {
          val jsonAst = responseAs[String].parseJson
          val jsonAsNodeAttrArray = jsonAst.convertTo[Iterable[NodeAttributes]]
          assert(jsonAsNodeAttrArray.isEmpty)
        }
      }
    }
  }
}
