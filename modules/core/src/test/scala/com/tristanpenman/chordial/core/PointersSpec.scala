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
import scala.language.postfixOps

final class PointersSpec
    extends TestKit(ActorSystem("PointersSpec"))
    with WordSpecLike
    with ImplicitSender
    with ScalaFutures {

  implicit val timeout = Timeout(2000.milliseconds)

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
        newPointersActor ! GetId()
        expectMsg(GetIdOk(1L))
      }

      "respond to a GetPredecessor message with a GetPredecessorOkButUnknown message" in {
        newPointersActor ! GetPredecessor
        expectMsg(GetPredecessorOkButUnknown)
      }

      "respond to a GetSuccessorList message with a GetSuccessorListOk message containing its successor's ID and " +
        "an empty backup successor list" in {
        newPointersActor ! GetSuccessorList
        expectMsg(GetSuccessorListOk(NodeInfo(seedId, dummyActorRef), List.empty))
      }
    }

    "its predecessor has been updated" should {
      def newPointersActor: ActorRef = {
        val actor =
          system.actorOf(Pointers.props(ownId, keyspaceBits, NodeInfo(seedId, dummyActorRef), system.eventStream))

        // Set predecessor pointer
        val future = actor.ask(UpdatePredecessor(NodeInfo(0L, self)))

        // Wait for update to be acknowledged
        assert(future.futureValue == UpdatePredecessorOk())
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
        expectMsg(ResetPredecessorOk())

        pointersActor ! GetPredecessor
        expectMsg(GetPredecessorOkButUnknown)
      }
    }

    "its successor list has been updated" should {
      val successorId = 3L
      val backupId = 4L
      val primarySuccessor = NodeInfo(successorId, dummyActorRef)
      val backupSuccessorList = List(NodeInfo(backupId, dummyActorRef))

      def newPointersActor: ActorRef = {
        val actor =
          system.actorOf(Pointers.props(ownId, keyspaceBits, NodeInfo(seedId, dummyActorRef), system.eventStream))

        // Update successor list
        val future =
          actor.ask(UpdateSuccessorList(primarySuccessor, backupSuccessorList))

        // Wait for update to be acknowledged
        assert(future.futureValue == UpdateSuccessorListOk)
        actor
      }

      "respond to a GetSuccessorList message with a GetSuccessorListOk message containing information for the " +
        "primary successor and the list of backup successors" in {
        newPointersActor ! GetSuccessorList
        expectMsg(GetSuccessorListOk(primarySuccessor, backupSuccessorList))
      }

      "respond to an UpdateSuccessorList message with an UpdateSuccessorListOk message, and respond to a subsequent " +
        "GetSuccessorList message with the new successor list" in {
        val pointersActor = newPointersActor
        val newSuccessorId = 2L
        val secondBackupId = 5L
        val newPrimarySuccessor = NodeInfo(newSuccessorId, dummyActorRef)
        val newBackupSuccessorList =
          List(NodeInfo(backupId, dummyActorRef), NodeInfo(secondBackupId, dummyActorRef))

        pointersActor ! UpdateSuccessorList(newPrimarySuccessor, newBackupSuccessorList)
        expectMsg(UpdateSuccessorListOk)

        pointersActor ! GetSuccessorList
        expectMsg(GetSuccessorListOk(newPrimarySuccessor, newBackupSuccessorList))
      }
    }
  }
}
