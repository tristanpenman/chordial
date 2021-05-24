package com.tristanpenman.chordial.core.algorithms

import java.net.InetSocketAddress

import akka.actor.{Actor, ActorRef, ActorSystem}
import akka.testkit.{ImplicitSender, TestActorRef, TestKit}
import akka.util.Timeout
import com.tristanpenman.chordial.core.Pointers.{GetSuccessor, GetSuccessorOk}
import com.tristanpenman.chordial.core.algorithms.ClosestPrecedingNodeAlgorithm._
import com.tristanpenman.chordial.core.shared.NodeInfo

import scala.collection.immutable.SortedMap
import scala.concurrent.duration._
import org.scalatest.wordspec.AnyWordSpecLike

final class ClosestPrecedingNodeAlgorithmSpec
    extends TestKit(ActorSystem("CheckPredecessorAlgorithmSpec"))
    with AnyWordSpecLike
    with ImplicitSender {

  // Timeout for requests performed within CheckPredecessorAlgorithm actor
  private val algorithmTimeout = Timeout(300.milliseconds)

  // Time to wait before concluding that no additional messages will be received
  private val spuriousMessageDuration = 150.milliseconds

  // Function to create a new ClosestPrecedingNodeAlgorithm actor using a given ActorRef as the Pointers actor
  private def newAlgorithmActor(node: NodeInfo, pointersRef: ActorRef) =
    system.actorOf(ClosestPrecedingNodeAlgorithm.props(node, pointersRef, algorithmTimeout))

  // Actor that will always discard messages
  private val unresponsiveActor = TestActorRef(new Actor {
    override def receive: Receive = {
      case _ =>
    }
  })

  private val dummyAddr = new InetSocketAddress("0.0.0.0", 0)

  // Actor that will cause test to fail if it receives any messages
  private val dummyActorRef: ActorRef = TestActorRef(new Actor {
    def receive: Receive = {
      case message =>
        fail(s"Dummy actor should not receive any messages, but just received: $message")
    }
  })

  "A ClosestPrecedingNodeAlgorithm actor" when {
    "initialised with a Node ID of 1 and a Pointers actor that only knows about Node ID 1" should {
      def newPointersActor: ActorRef =
        TestActorRef(new Actor {
          def failOnReceive: Receive = {
            case m =>
              fail(s"Pointers actor received an unexpected message of type: ${m.getClass})")
          }

          override def receive: Receive = {
            case GetSuccessor =>
              sender() ! GetSuccessorOk(NodeInfo(1L, dummyAddr, dummyActorRef))
              context.become(failOnReceive)
            case m =>
              fail(s"Pointers actor received an unexpected message of type: ${m.getClass})")
          }
        })

      val testCases = 0 to 4

      testCases.foreach { input =>
        "return a pointer for Node with ID 1 when queried for ID " + input in {
          val algorithm = newAlgorithmActor(NodeInfo(1L, dummyAddr, dummyActorRef), newPointersActor)
          algorithm ! ClosestPrecedingNodeAlgorithmStart(input)
          expectMsg(ClosestPrecedingNodeAlgorithmFinished(NodeInfo(1L, dummyAddr, dummyActorRef)))
        }
      }

      "finish without sending any further messages" in {
        val algorithm = newAlgorithmActor(NodeInfo(1L, dummyAddr, dummyActorRef), newPointersActor)
        algorithm ! ClosestPrecedingNodeAlgorithmStart(1L)
        expectMsgType[ClosestPrecedingNodeAlgorithmFinished]
        expectNoMessage(spuriousMessageDuration)
      }
    }

    "initialised with a Node ID of 1 and a Pointers actor that has one finger table entry, with ID 3" should {
      val nodeId = 1L
      val successorId = 3L
      def newPointersActor: ActorRef =
        TestActorRef(new Actor {
          override def receive: Receive = {
            case GetSuccessor =>
              sender() ! GetSuccessorOk(NodeInfo(successorId, dummyAddr, dummyActorRef))
          }
        })

      val testCases = SortedMap(0L -> 3L, 1L -> 3L, 2L -> 1L, 3L -> 1L, 4L -> 3L)

      testCases.foreach {
        case (input, output) =>
          "return a pointer for Node with ID " + output + " when queried for ID " + input in {
            val algorithm = newAlgorithmActor(NodeInfo(nodeId, dummyAddr, dummyActorRef), newPointersActor)
            algorithm ! ClosestPrecedingNodeAlgorithmStart(input)
            expectMsg(ClosestPrecedingNodeAlgorithmFinished(NodeInfo(output, dummyAddr, dummyActorRef)))
          }
      }

      "not send any additional messages after finishing" in {
        val algorithm = newAlgorithmActor(NodeInfo(nodeId, dummyAddr, dummyActorRef), newPointersActor)
        algorithm ! ClosestPrecedingNodeAlgorithmStart(1L)
        expectMsgType[ClosestPrecedingNodeAlgorithmFinished]
        expectNoMessage(spuriousMessageDuration)
      }
    }

    "initialised with a Node ID of 3 and a Pointers actor that has one finger table entry, with ID 1" should {
      val nodeId = 3L
      val successorId = 1L
      def newPointersActor: ActorRef =
        TestActorRef(new Actor {
          override def receive: Receive = {
            case GetSuccessor =>
              sender() ! GetSuccessorOk(NodeInfo(successorId, dummyAddr, dummyActorRef))
          }
        })

      val testCases = SortedMap(0L -> 3L, 1L -> 3L, 2L -> 1L, 3L -> 1L, 4L -> 3L)

      testCases.foreach {
        case (input, output) =>
          "return a pointer for Node with ID " + output + " when queried for ID " + input in {
            val algorithm = newAlgorithmActor(NodeInfo(nodeId, dummyAddr, dummyActorRef), newPointersActor)
            algorithm ! ClosestPrecedingNodeAlgorithmStart(input)
            expectMsg(ClosestPrecedingNodeAlgorithmFinished(NodeInfo(output, dummyAddr, dummyActorRef)))
          }
      }

      "not send any additional messages after finishing" in {
        val algorithm = newAlgorithmActor(NodeInfo(nodeId, dummyAddr, dummyActorRef), newPointersActor)
        algorithm ! ClosestPrecedingNodeAlgorithmStart(1L)
        expectMsgType[ClosestPrecedingNodeAlgorithmFinished]
        expectNoMessage(spuriousMessageDuration)
      }
    }

    "initialised with a Pointers actor that is unresponsive" should {
      "return an error" in {
        val algorithm = newAlgorithmActor(NodeInfo(1L, dummyAddr, dummyActorRef), unresponsiveActor)
        algorithm ! ClosestPrecedingNodeAlgorithmStart(1L)
        expectMsgType[ClosestPrecedingNodeAlgorithmError]
        expectNoMessage(spuriousMessageDuration)
      }
    }
  }
}
