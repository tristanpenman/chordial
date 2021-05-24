package com.tristanpenman.chordial.core

import akka.actor.{Actor, ActorRef, ActorSystem}
import akka.pattern.ask
import akka.testkit.{ImplicitSender, TestActorRef, TestKit}
import akka.util.Timeout
import com.tristanpenman.chordial.core.Pointers._
import com.tristanpenman.chordial.core.shared.NodeInfo
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.wordspec.AnyWordSpecLike

import java.net.InetSocketAddress
import scala.concurrent.duration._

final class PointersSpec
    extends TestKit(ActorSystem("PointersSpec"))
    with AnyWordSpecLike
    with ImplicitSender
    with ScalaFutures {

  implicit val timeout: Timeout = Timeout(2000.milliseconds)

  private val ownId = 1L
  private val seedId = 2L
  private val keyspaceBits = 3

  private val dummyActorRef: ActorRef = TestActorRef(new Actor {
    def receive: Receive = {
      case message =>
        fail(s"Dummy actor should not receive any messages, but just received: $message")
    }
  })

  private val dummyAddr = new InetSocketAddress("0.0.0.0", 0)
  private val dummySeedInfo = NodeInfo(seedId, dummyAddr, dummyActorRef)

  "A Pointers actor" when {

    "initially constructed" should {
      def newPointersActor: ActorRef =
        system.actorOf(
          Pointers.props(ownId, keyspaceBits, NodeInfo(seedId, dummyAddr, dummyActorRef), system.eventStream))

      "respond to a GetId message with a GetIdOk message containing its ID" in {
        newPointersActor ! GetId
        expectMsg(GetIdOk(1L))
      }

      "respond to a GetPredecessor message with a GetPredecessorOkButUnknown message" in {
        newPointersActor ! GetPredecessor
        expectMsg(GetPredecessorOkButUnknown)
      }

      "respond to a GetSuccessor message with a GetSuccessorOk message containing its successor's ID" in {
        newPointersActor ! GetSuccessor
        expectMsg(GetSuccessorOk(dummySeedInfo))
      }
    }

    "its predecessor has been updated" should {
      def newPointersActor: ActorRef = {
        val actor =
          system.actorOf(Pointers.props(ownId, keyspaceBits, dummySeedInfo, system.eventStream))

        // Set predecessor pointer
        val future = actor.ask(UpdatePredecessor(NodeInfo(0L, dummyAddr, self)))

        // Wait for update to be acknowledged
        assert(future.futureValue == UpdatePredecessorOk)
        actor
      }

      "respond to a GetPredecessor message with a GetPredecessorOk message containing its predecessor's ID" in {
        newPointersActor ! GetPredecessor
        expectMsg(GetPredecessorOk(NodeInfo(0L, dummyAddr, self)))
      }

      "respond to a ResetPredecessor message with a ResetPredecessorOk message, and respond to a subsequent " +
        "GetPredecessor message with a GetPredecessorOkButUnknown message" in {
        val pointersActor = newPointersActor

        pointersActor ! ResetPredecessor
        expectMsg(ResetPredecessorOk)

        pointersActor ! GetPredecessor
        expectMsg(GetPredecessorOkButUnknown)
      }
    }

    "its successor list has been updated" should {
      val successorId = 3L
      val successor = NodeInfo(successorId, dummyAddr, dummyActorRef)

      def newPointersActor: ActorRef = {
        val actor =
          system.actorOf(Pointers.props(ownId, keyspaceBits, dummySeedInfo, system.eventStream))

        // Update successor list
        val future = actor.ask(UpdateSuccessor(successor))

        // Wait for update to be acknowledged
        assert(future.futureValue == UpdateSuccessorOk)
        actor
      }

      "respond to a GetSuccessor message with a GetSuccessorOk message" in {
        newPointersActor ! GetSuccessor
        expectMsg(GetSuccessorOk(successor))
      }

      "respond to an UpdateSuccessor message with an UpdateSuccessorOk message, and respond to a subsequent " +
        "GetSuccessor message with the new successor" in {
        val pointersActor = newPointersActor
        val newSuccessorId = 2L
        val newSuccessor = NodeInfo(newSuccessorId, dummyAddr, dummyActorRef)

        pointersActor ! UpdateSuccessor(newSuccessor)
        expectMsg(UpdateSuccessorOk)

        pointersActor ! GetSuccessor
        expectMsg(GetSuccessorOk(newSuccessor))
      }
    }
  }
}
