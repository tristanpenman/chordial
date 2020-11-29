package com.tristanpenman.chordial.core.algorithms

import java.net.InetSocketAddress

import akka.actor.{Actor, ActorRef, ActorSystem}
import akka.pattern.ask
import akka.testkit.{ImplicitSender, TestActorRef, TestKit}
import akka.util.Timeout
import com.tristanpenman.chordial.core.Pointers._
import com.tristanpenman.chordial.core.Router
import com.tristanpenman.chordial.core.Router.{Start, StartFailed, StartOk, StartResponse}
import com.tristanpenman.chordial.core.algorithms.CheckPredecessorAlgorithm._
import com.tristanpenman.chordial.core.shared.NodeInfo
import org.scalatest.WordSpecLike

import scala.concurrent.{Await, ExecutionContextExecutor}
import scala.concurrent.duration._

final class CheckPredecessorAlgorithmSpec
    extends TestKit(ActorSystem("CheckPredecessorAlgorithmSpec"))
    with WordSpecLike
    with ImplicitSender {

  implicit val ec: ExecutionContextExecutor = system.dispatcher

  // Timeout for requests performed within CheckPredecessorAlgorithm actor
  private val defaultTimeout = Timeout(300.milliseconds)

  // Time to wait before concluding that no additional messages will be received
  private val spuriousMessageDuration = 150.milliseconds

  private val dummyAddr = new InetSocketAddress("0.0.0.0", 0)

  private val router = {
    val routerRef = system.actorOf(Router.props(Map.empty))

    Await.result(
      routerRef
        .ask(Start("0.0.0.0", 0))(defaultTimeout)
        .mapTo[StartResponse]
        .map {
          case StartOk(_) =>
            routerRef
          case StartFailed(reason) =>
            throw new Exception(s"Failed to start Router: ${reason}")
        },
      Duration.Inf
    )
  }

  // Function to create a new CheckPredecessorAlgorithm actor using a given ActorRef as the Pointers actor
  private def newAlgorithmActor(pointersActor: ActorRef): ActorRef =
    system.actorOf(CheckPredecessorAlgorithm.props(router, pointersActor, defaultTimeout))

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
            case GetSuccessor =>
              sender() ! GetSuccessorOk(NodeInfo(1L, dummyAddr, self))
          }
        })

        TestActorRef(new Actor {
          override def receive: Receive = {
            case GetPredecessor =>
              sender() ! GetPredecessorOk(NodeInfo(1L, dummyAddr, healthyPredecessor))
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
              sender() ! GetPredecessorOk(NodeInfo(1L, dummyAddr, unresponsiveActor))
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
