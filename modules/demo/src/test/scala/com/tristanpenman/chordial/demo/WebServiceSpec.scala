package com.tristanpenman.chordial.demo

import akka.actor.{Actor, ActorRef, ActorSystem}
import akka.testkit.TestActorRef
import com.tristanpenman.chordial.demo.Governor._
import org.scalatest.WordSpec
import org.scalatest.Matchers._
import spray.http.StatusCodes
import spray.json._
import spray.testkit.ScalatestRouteTest

class WebServiceSpec extends WordSpec with WebService with ScalatestRouteTest {

  import WebService._

  def actorRefFactory: ActorSystem = system

  private val dummyActor: ActorRef = TestActorRef(new Actor {
    def receive: Receive = {
      case message =>
        fail(s"Dummy actor should not receive any messages, but just received: $message")
    }
  })

  "The web service" when {

    "backed by a Governor with no nodes" should {
      def newGovernor: ActorRef = TestActorRef(new Actor {
        def receive: Receive = {
          case CreateNode() =>
            sender() ! CreateNodeOk(1L, dummyActor)
          case CreateNodeWithSeed(seedId) =>
            sender() ! CreateNodeWithSeedOk(2L, dummyActor)
          case GetNodeIdSet() =>
            sender() ! GetNodeIdSetOk(Set.empty)
        }
      })

      "respond to a GET request on the /nodes endpoint with an empty JSON array" in {
        Get("/nodes") ~> routes(newGovernor) ~> check {
          assert(response.status == StatusCodes.OK)
          val jsonAst = responseAs[String].parseJson
          val jsonAsNodeAttrArray = jsonAst.convertTo[Iterable[NodeAttributes]]
          assert(jsonAsNodeAttrArray.isEmpty)
        }
      }

      "respond to a POST request on the /nodes endpoint, which does not include a seed ID, with the correct JSON " +
        "object" in {
        Post("/nodes") ~> routes(newGovernor) ~> check {
          assert(response.status == StatusCodes.OK)
          val jsonAst = responseAs[String].parseJson
          val jsonAsNodeAttr = jsonAst.convertTo[NodeAttributes]
          assert(jsonAsNodeAttr.nodeId == 1L)
          assert(jsonAsNodeAttr.successorId.contains(1L))
          assert(jsonAsNodeAttr.active)
        }
      }

      "respond to a POST request on the /nodes endpoint, which includes a seed ID, with the correct JSON object" in {
        Post("/nodes?seed_id=1") ~> routes(newGovernor) ~> check {
          assert(response.status == StatusCodes.OK)
          val jsonAst = responseAs[String].parseJson
          val jsonAsNodeAttr = jsonAst.convertTo[NodeAttributes]
          assert(jsonAsNodeAttr.nodeId == 2L)
          assert(jsonAsNodeAttr.successorId.contains(1L))
          assert(jsonAsNodeAttr.active)
        }
      }
    }

    "backed by a Governor with existing nodes" should {
      def newGovernor: ActorRef = TestActorRef(new Actor {
        def receive: Receive = {
          case GetNodeIdSet() =>
            sender() ! GetNodeIdSetOk(Set(0L, 1L, 2L))
          case GetNodeState(nodeId) =>
            sender() ! GetNodeStateOk(nodeId == 0L || nodeId == 1L)
          case GetNodeSuccessorId(nodeId) =>
            if (nodeId == 2L) {
              fail("Web service attempted to query successor ID of an inactive node")
            } else {
              sender() ! GetNodeSuccessorIdOk((nodeId + 1L) % 2L)
            }
        }
      })

      "respond to a GET request on the /nodes endpoint with a JSON array of correct JSON objects" in {
        Get("/nodes") ~> routes(newGovernor) ~> check {
          assert(response.status == StatusCodes.OK)
          val jsonAst = responseAs[String].parseJson
          val jsonAsNodeAttrArray = jsonAst.convertTo[Array[NodeAttributes]]
          assert(jsonAsNodeAttrArray.length == 3)
          assert(jsonAsNodeAttrArray.contains(NodeAttributes(0L, Some(1L), active = true)))
          assert(jsonAsNodeAttrArray.contains(NodeAttributes(1L, Some(0L), active = true)))
          assert(jsonAsNodeAttrArray.contains(NodeAttributes(2L, None, active = false)))
        }
      }

      "respond to a GET request on the /nodes/{node_id} endpoint, where {node_id} is valid and the node is active, " +
        "with a JSON object" in {
        Get("/nodes/0") ~> routes(newGovernor) -> check {
          assert(response.status == StatusCodes.OK)
          val jsonAst = responseAs[String].parseJson
          val jsonAsNodeAttrs = jsonAst.convertTo[NodeAttributes]
          assert(jsonAsNodeAttrs == NodeAttributes(0L, Some(1L), active = true))
        }
      }

      "respond to a GET request on the /nodes/{node_id} endpoint, where {node_id} is valid but the node is inactive, " +
        "with a JSON object" in {
        Get("/nodes/2") ~> routes(newGovernor) -> check {
          assert(response.status == StatusCodes.OK)
          val jsonAst = responseAs[String].parseJson
          val jsonAsNodeAttrs = jsonAst.convertTo[NodeAttributes]
          assert(jsonAsNodeAttrs == NodeAttributes(2L, None, active = false))
        }
      }

      "respond to a GET request on the /nodes/{node_id} endpoint, where {node_id} is not valid, with a 400 status" in {
        List("3", "-1", "-0", "a", ".", "..", "../", "~", "!", "+").foreach(nodeId =>
          Get("/nodes/" + nodeId) -> routes(newGovernor) -> check {
            assert(response.status == StatusCodes.BadRequest)
          }
        )
      }
    }

    "backed by a Governor that reports an internal error when creating nodes" should {
      def newGovernor: ActorRef = TestActorRef(new Actor {
        def receive: Receive = {
          case CreateNode() =>
            sender() ! CreateNodeInternalError("Dummy message")
          case CreateNodeWithSeed(_) =>
            sender() ! CreateNodeWithSeedInternalError("Dummy message")
        }
      })

      "respond to a POST request on the /nodes endpoint, which does not include a seed ID, with a 500 status but " +
        "not the original error message" in {
        Post("/nodes") ~> routes(newGovernor) ~> check {
          assert(response.status == StatusCodes.InternalServerError)
          assert(!responseAs[String].contains("Dummy message"))
        }
      }

      "respond to a POST request on the /nodes endpoint, which includes a seed ID, with a 500 status but not the " +
        "original error message" in {
        Post("/nodes?seed_id=1") ~> routes(newGovernor) ~> check {
          assert(response.status == StatusCodes.InternalServerError)
          assert(!responseAs[String].contains("Dummy message"))
        }
      }
    }

    "backed by a Governor that reports an internal error when checking the state of a node" should {
      def newGovernor: ActorRef = TestActorRef(new Actor {
        def receive: Receive = {
          case GetNodeIdSet() =>
            sender() ! GetNodeIdSetOk(Set(0L, 1L, 2L))
          case GetNodeState(_) =>
            sender() ! GetNodeStateError("Dummy message")
        }
      })

      "respond to a GET request on the /nodes endpoint with a 500 status but not the original error message" in {
        Get("/nodes") ~> routes(newGovernor) ~> check {
          assert(response.status == StatusCodes.InternalServerError)
          assert(!responseAs[String].contains("Dummy message"))
        }
      }
    }

    "backed by a Governor that reports an internal error when retrieving the successor ID of a node" should {
      def newGovernor: ActorRef = TestActorRef(new Actor {
        def receive: Receive = {
          case GetNodeIdSet() =>
            sender() ! GetNodeIdSetOk(Set(0L, 1L, 2L))
          case GetNodeState(_) =>
            sender() ! GetNodeStateOk(active = true)
          case GetNodeSuccessorId(_) =>
            sender() ! GetNodeSuccessorIdError("Dummy message")
        }
      })

      "respond to a GET request on the /nodes endpoint with a 500 status but not the original error message" in {
        Get("/nodes") ~> routes(newGovernor) ~> check {
          assert(response.status == StatusCodes.InternalServerError)
          assert(!responseAs[String].contains("Dummy message"))
        }
      }
    }
  }
}
