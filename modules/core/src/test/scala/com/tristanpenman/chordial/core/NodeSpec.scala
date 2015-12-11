package com.tristanpenman.chordial.core

import akka.actor.{ActorRef, ActorSystem}
import akka.testkit.{ImplicitSender, TestKit}
import com.tristanpenman.chordial.core.Node._
import com.tristanpenman.chordial.core.shared.NodeInfo
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

class NodeSpec(_system: ActorSystem) extends TestKit(_system) with ImplicitSender with WordSpecLike with Matchers
with BeforeAndAfterAll {

  def this() = this(ActorSystem("NodeSpec"))

  val keyspaceBits = 3

  override def afterAll() {
    TestKit.shutdownActorSystem(system)
  }

  "A Node actor" must {
    val ownId = 1L
    val seedId = 2L
    val seedRef = self

    val node: ActorRef = system.actorOf(Node.props(ownId, keyspaceBits, NodeInfo(seedId, seedRef), system.eventStream))

    "respond to a GetId message with a GetIdOk message containing its ID" in {
      node ! GetId()
      expectMsg(GetIdOk(ownId))
    }

    "respond to a GetPredecessor message with a GetPredecessorOkButUnknown message" in {
      node ! GetPredecessor()
      expectMsg(GetPredecessorOkButUnknown())
    }

    "respond to a GetSuccessor message with a GetSuccessorOk message containing the ID and ActorRef of a seed node" in {
      node ! GetSuccessor()
      expectMsg(GetSuccessorOk(seedId, seedRef))
    }
  }

}
