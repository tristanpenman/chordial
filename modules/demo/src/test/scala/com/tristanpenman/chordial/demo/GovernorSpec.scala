package com.tristanpenman.chordial.demo

import akka.actor.{ActorRef, ActorSystem}
import akka.testkit.{ImplicitSender, TestKit}
import com.tristanpenman.chordial.demo.Governor._
import org.scalatest._

import scala.concurrent.duration._

final class GovernorSpec extends TestKit(ActorSystem("GovernorSpec")) with WordSpecLike with ImplicitSender {

  // Time to wait before concluding that no additional messages will be received
  private val spuriousMessageDuration = 150.milliseconds

  // Limit size of keyspace / number of nodes to 2^3 == 8
  private val keyspaceBits = 3

  // Function to return the ID of an arbitrarily chosen Node owned by a given Governor actor
  private def getFirstNodeId(governor: ActorRef): Long = {
    governor ! GetNodeIdSet
    expectMsgPF() {
      case GetNodeIdSetOk(nodeIds) =>
        if (nodeIds.isEmpty) {
          fail("Governor returned an empty set of Node IDs")
        } else {
          nodeIds.head
        }
    }
  }

  "A Governor actor" when {
    "initially constructed" should {
      def newGovernor: ActorRef = system.actorOf(Governor.props(keyspaceBits))

      "respond to a CreateNode message with a CreateNodeOk message, and then no further messages" in {
        newGovernor ! CreateNode
        expectMsgType[CreateNodeOk]
        expectNoMessage(spuriousMessageDuration)
      }

      "respond to two CreateNode messages with two unique CreateNodeOk messages, and then no further messages" in {
        val governor = newGovernor
        governor ! CreateNode
        expectMsgPF() {
          case CreateNodeOk(firstNodeId, _) =>
            governor ! CreateNode
            expectMsgPF() {
              case CreateNodeOk(secondNodeId, _) =>
                assert(firstNodeId != secondNodeId)
            }
        }
      }
    }

    "already at its Node capacity" should {
      def newGovernor: ActorRef = {
        val governor = system.actorOf(Governor.props(keyspaceBits))
        1 to (1 << keyspaceBits) foreach {
          case _ =>
            governor ! CreateNode
            expectMsgType[CreateNodeOk]
        }
        governor
      }

      "respond to a CreateNode message with a CreateNodeInvalidRequest message" in {
        newGovernor ! CreateNode
        expectMsgType[CreateNodeInvalidRequest]
      }

      "respond to a CreateNodeWithSeed message with a CreateNodeWithSeedInvalidRequest message" in {
        val governor = newGovernor
        val seedId = getFirstNodeId(governor)
        governor ! CreateNodeWithSeed(seedId)
        expectMsgType[CreateNodeWithSeedInvalidRequest]
      }
    }
  }
}
