package com.tristanpenman.chordial.core.algorithms

import akka.actor.{Actor, ActorRef, ActorSystem}
import akka.testkit.{TestProbe, ImplicitSender, TestActorRef, TestKit}
import akka.util.Timeout
import com.tristanpenman.chordial.core.Node.{Notify, NotifyOk}
import com.tristanpenman.chordial.core.Pointers
import com.tristanpenman.chordial.core.Pointers._
import com.tristanpenman.chordial.core.algorithms.StabilisationAlgorithm.{StabilisationAlgorithmFinished, StabilisationAlgorithmStart}
import com.tristanpenman.chordial.core.shared.NodeInfo
import org.scalatest.WordSpecLike

import scala.concurrent.duration._
import scala.language.postfixOps

class StabilisationAlgorithmSpec
  extends TestKit(ActorSystem("StabilisationAlgorithmSpec")) with WordSpecLike with ImplicitSender {

  private val keyspaceBits = 3

  // Timeout for requests performed within CheckPredecessorAlgorithm actor
  private val algorithmTimeout = Timeout(300.milliseconds)

  // Time to wait before concluding that no additional messages will be received
  private val spuriousMessageDuration = 150.milliseconds

  private val livenessCheckDuration = 100.milliseconds

  // Actor that will cause test to fail if it receives any messages
  private val dummyActorRef: ActorRef = TestActorRef(new Actor {
    def receive: Receive = {
      case message =>
        fail(s"Dummy actor should not receive any messages, but just received: $message")
    }
  })

  "A StabilisationAlgorithm actor" when {
    "initialised with a Pointers actor for a node that is its own successor" should {
      def newAlgorithm: ActorRef = {
        val nodeRef = TestActorRef(new Actor {
          override def receive: Receive = {
            case GetPredecessor() =>
              sender() ! GetPredecessorOk(NodeInfo(1L, self))
            case GetSuccessorList() =>
              sender() ! GetSuccessorListOk(NodeInfo(1L, self), List.empty)
            case Notify(_, _) =>
              sender() ! NotifyOk()
            case m =>
              fail(s"Stub Node actor received an unexpected message of type: ${m.getClass})")
          }
        })

        val pointersRef = system.actorOf(Pointers.props(1L, keyspaceBits, NodeInfo(1L, nodeRef), system.eventStream))

        system.actorOf(StabilisationAlgorithm.props(NodeInfo(1L, nodeRef), pointersRef, algorithmTimeout))
      }

      "finish successfully without sending any further messages" in {
        newAlgorithm ! StabilisationAlgorithmStart()
        expectMsg(StabilisationAlgorithmFinished())
        expectNoMsg(spuriousMessageDuration)
      }
    }

    "initialised with a Pointers actor with two successors, where the first node is failing to respond to " +
      "GetPredecessor messages" should {
      "finish successfully after removing the failed node from the Pointers actor's successor list" in {
        val node2 = TestActorRef(new Actor {
          override def receive: Receive = {
            case GetPredecessor() =>
              // Ignored to simulate failure
            case m =>
              fail(s"Stub Node actor for ID 2 received an unexpected message of type: ${m.getClass})")
          }
        })

        val node3 = TestActorRef(new Actor {
          override def receive: Receive = {
            case GetPredecessor() =>
              sender() ! GetPredecessorOk(NodeInfo(2L, node2))
            case GetSuccessorList() =>
              // StabilisationAlgorithm will use this node's successor list to update node 1's successor list
              sender() ! GetSuccessorListOk(NodeInfo(1L, dummyActorRef), List(NodeInfo(2L, node2), NodeInfo(3L, self)))
            case Notify(_, _) =>
              // StabilisationAlgorithm will notify this node that it is node 1's new successor
              sender() ! NotifyOk()
            case m =>
              fail(s"Stub Node actor for ID 3 received an unexpected message of type: ${m.getClass})")
          }
        })

        // The Pointers actor, for node ID 1, that will be modified in response to failure
        val pointersRef = TestActorRef(Pointers.props(1L, keyspaceBits, NodeInfo(2L, node2), system.eventStream))
        pointersRef ! UpdatePredecessor(NodeInfo(3L, node3))
        expectMsg(UpdatePredecessorOk())
        pointersRef ! UpdateSuccessorList(NodeInfo(2L, node2), List(NodeInfo(3L, node3), NodeInfo(1L, dummyActorRef)))
        expectMsg(UpdateSuccessorListOk())

        // Test probe to ensure that the correct messages are sent to Pointers actor
        val pointersTestProbe = new TestProbe(system)

        val algorithm = system.actorOf(
          StabilisationAlgorithm.props(NodeInfo(1L, dummyActorRef), pointersTestProbe.ref, algorithmTimeout))
        algorithm ! StabilisationAlgorithmStart()

        // StabilisationAlgorithm actor should ask Pointers actor for current successor list
        pointersTestProbe.expectMsg(GetSuccessorList())
        pointersTestProbe.forward(pointersRef)

        // After node 2 fails to respond to a GetPredecessor message, StabilisationActor should ask Pointers actor to
        // remove node 2 from the its successor list
        pointersTestProbe.expectMsg(
          UpdateSuccessorList(NodeInfo(3L, node3), List(NodeInfo(1L, dummyActorRef))))
        pointersTestProbe.forward(pointersRef)

        // The StabilisationAlgorithm actor will then reconcile node 1's successor list with node 3's successor list,
        // leaving out node 2 since it is already known to have failed
        pointersTestProbe.expectMsg(
          UpdateSuccessorList(NodeInfo(3L, node3), List(NodeInfo(1L, dummyActorRef))))
        pointersTestProbe.forward(pointersRef)

        expectMsg(StabilisationAlgorithmFinished())
        expectNoMsg(spuriousMessageDuration)
        pointersTestProbe.expectNoMsg(spuriousMessageDuration)
      }
    }
  }
}
