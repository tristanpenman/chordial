package com.tristanpenman.chordial.core

import akka.actor.{Actor, ActorRef, ActorSystem}
import akka.pattern.ask
import akka.testkit.{ImplicitSender, TestActorRef, TestKit}
import akka.util.Timeout
import com.tristanpenman.chordial.core.Pointers._
import com.tristanpenman.chordial.core.shared.NodeInfo
import org.scalatest._
import org.scalatest.concurrent.ScalaFutures

import scala.concurrent.duration._

final class PointersSpec
    extends TestKit(ActorSystem("PointersSpec"))
    with WordSpecLike
    with ImplicitSender
    with ScalaFutures {

  implicit val timeout: Timeout = Timeout(2000.milliseconds)

  private val dummyActorRef: ActorRef = TestActorRef(new Actor {
    def receive: Receive = {
      case message =>
        fail(s"Dummy actor should not receive any messages, but just received: $message")
    }
  })

  private val ownId = 1L
  private val seedId = 2L
  private val keyspaceBits = 3

  "A Pointers actor" when {

    "initially constructed" should {
      def newPointersActor: ActorRef =
        system.actorOf(Pointers.props(ownId, keyspaceBits, NodeInfo(seedId, dummyActorRef), system.eventStream))

      "respond to a GetId message with a GetIdOk message containing its ID" in {
        newPointersActor ! GetId
        expectMsg(GetIdOk(1L))
      }

      "respond to a GetPredecessor message with a GetPredecessorOkButUnknown message" in {
        newPointersActor ! GetPredecessor
        expectMsg(GetPredecessorOkButUnknown)
      }

      "respond to a GetSuccessor message with a GetSuccessorOk message containing its successor's ID and " +
        "an empty backup successor list" in {
        newPointersActor ! GetSuccessor
        expectMsg(GetSuccessorOk(NodeInfo(seedId, dummyActorRef)))
      }
    }

    "its predecessor has been updated" should {
      def newPointersActor: ActorRef = {
        val actor =
          system.actorOf(Pointers.props(ownId, keyspaceBits, NodeInfo(seedId, dummyActorRef), system.eventStream))

        // Set predecessor pointer
        val future = actor.ask(UpdatePredecessor(NodeInfo(0L, self)))

        // Wait for update to be acknowledged
        assert(future.futureValue == UpdatePredecessorOk)
        actor
      }

      "respond to a GetPredecessor message with a GetPredecessorOk message containing its predecessor's ID" in {
        newPointersActor ! GetPredecessor
        expectMsg(GetPredecessorOk(NodeInfo(0L, self)))
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
      val successor = NodeInfo(successorId, dummyActorRef)

      def newPointersActor: ActorRef = {
        val actor =
          system.actorOf(Pointers.props(ownId, keyspaceBits, NodeInfo(seedId, dummyActorRef), system.eventStream))

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
        val newSuccessor = NodeInfo(newSuccessorId, dummyActorRef)

        pointersActor ! UpdateSuccessor(newSuccessor)
        expectMsg(UpdateSuccessorOk)

        pointersActor ! GetSuccessor
        expectMsg(GetSuccessorOk(newSuccessor))
      }
    }
  }
}
