package com.tristanpenman.chordial.core.algorithms

import akka.actor.{Actor, ActorRef, ActorSystem}
import akka.testkit.{ImplicitSender, TestActorRef, TestKit}
import akka.util.Timeout
import com.tristanpenman.chordial.core.Pointers._
import com.tristanpenman.chordial.core.algorithms.CheckPredecessorAlgorithm._
import com.tristanpenman.chordial.core.shared.NodeInfo
import org.scalatest.WordSpecLike

import scala.concurrent.duration._

final class CheckPredecessorAlgorithmSpec
    extends TestKit(ActorSystem("CheckPredecessorAlgorithmSpec"))
    with WordSpecLike
    with ImplicitSender {

  // Timeout for requests performed within CheckPredecessorAlgorithm actor
  private val algorithmTimeout = Timeout(300.milliseconds)

  // Time to wait before concluding that no additional messages will be received
  private val spuriousMessageDuration = 150.milliseconds

  // Function to create a new CheckPredecessorAlgorithm actor using a given ActorRef as the Pointers actor
  private def newAlgorithmActor(pointersActor: ActorRef): ActorRef =
    system.actorOf(CheckPredecessorAlgorithm.props(pointersActor, algorithmTimeout))

  // Actor that will always discard messages
  private val unresponsiveActor = TestActorRef(new Actor {
    override def receive: Receive = {
      case _ =>
    }
  })

  "A CheckPredecessorAlgorithm actor" when {
    "initialised with a Pointers actor that does not have a predecessor" should {
      def newPointersActor: ActorRef =
        TestActorRef(new Actor {
          def failOnReceive: Receive = {
            case m =>
              fail(s"Pointers actor received an unexpected message of type: ${m.getClass})")
          }

          override def receive: Receive = {
            case GetPredecessor =>
              sender() ! GetPredecessorOkButUnknown
              context.become(failOnReceive)
            case m =>
              fail(s"Pointers actor received an unexpected message of type: ${m.getClass})")
          }
        })

      "finish successfully without sending any further messages" in {
        newAlgorithmActor(newPointersActor) ! CheckPredecessorAlgorithmStart
        expectMsg(CheckPredecessorAlgorithmFinished)
        expectNoMessage(spuriousMessageDuration)
      }
    }

    "initialised with a Pointers actor whose predecessor is healthy" should {
      def newPointersActor: ActorRef = {
        val healthyPredecessor = TestActorRef(new Actor {
          override def receive: Receive = {
            case GetSuccessorList =>
              sender() ! GetSuccessorListOk(NodeInfo(1L, self), List.empty)
          }
        })

        TestActorRef(new Actor {
          override def receive: Receive = {
            case GetPredecessor =>
              sender() ! GetPredecessorOk(NodeInfo(1L, healthyPredecessor))
            case m =>
              fail(s"Pointers actor received an unexpected message of type: ${m.getClass})")
          }
        })
      }

      "finish successfully without sending any further messages" in {
        newAlgorithmActor(newPointersActor) ! CheckPredecessorAlgorithmStart
        expectMsg(CheckPredecessorAlgorithmFinished)
        expectNoMessage(spuriousMessageDuration)
      }
    }

    "initialised with a Pointers actor whose predecessor is unresponsive" should {
      def newPointersActor: ActorRef =
        TestActorRef(new Actor {
          def receiveWithUnknownPredecessor: Receive = {
            case GetPredecessor =>
              sender() ! GetPredecessorOkButUnknown
            case m =>
              fail(s"Pointers actor received an unexpected message of type: ${m.getClass})")
          }

          override def receive: Receive = {
            case GetPredecessor =>
              sender() ! GetPredecessorOk(NodeInfo(1L, unresponsiveActor))
            case ResetPredecessor =>
              sender() ! ResetPredecessorOk
              context.become(receiveWithUnknownPredecessor)
            case m =>
              fail(s"Pointers actor received an unexpected message of type: ${m.getClass})")
          }
        })

      "finish successfully after sending a ResetPredecessor message to the Pointer actor" in {
        val pointersActor = newPointersActor
        newAlgorithmActor(pointersActor) ! CheckPredecessorAlgorithmStart
        expectMsg(CheckPredecessorAlgorithmFinished)
        expectNoMessage(spuriousMessageDuration)
        pointersActor ! GetPredecessor
        expectMsg(GetPredecessorOkButUnknown)
      }
    }

    "initialised with a Pointers actor that is unresponsive" should {
      "finish with an error" in {
        newAlgorithmActor(unresponsiveActor) ! CheckPredecessorAlgorithmStart
        expectMsgType[CheckPredecessorAlgorithmError]
        expectNoMessage(spuriousMessageDuration)
      }
    }
  }
}
