package com.tristanpenman.chordial.core.algorithms

import java.net.InetSocketAddress

import akka.actor.{Actor, ActorRef, ActorSystem}
import akka.pattern.ask
import akka.testkit.{ImplicitSender, TestActorRef, TestKit}
import akka.util.Timeout
import com.tristanpenman.chordial.core.Pointers._
import com.tristanpenman.chordial.core.Router
import com.tristanpenman.chordial.core.Router.{
  Register,
  RegisterOk,
  RegisterResponse,
  Start,
  StartFailed,
  StartOk,
  StartResponse
}
import com.tristanpenman.chordial.core.algorithms.CheckPredecessorAlgorithm._
import com.tristanpenman.chordial.core.shared.NodeInfo

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContextExecutor}
import org.scalatest.wordspec.AnyWordSpecLike

final class CheckPredecessorAlgorithmSpec
    extends TestKit(ActorSystem("CheckPredecessorAlgorithmSpec"))
    with AnyWordSpecLike
    with ImplicitSender {

  implicit val ec: ExecutionContextExecutor = system.dispatcher

  // Timeout for requests performed within CheckPredecessorAlgorithm actor
  private val defaultTimeout = Timeout(300.milliseconds)

  // Time to wait before concluding that no additional messages will be received
  private val spuriousMessageDuration = 150.milliseconds

  private val dummyAddr = new InetSocketAddress("0.0.0.0", 0)

  private def newRouter = {
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

  private def registerNode(router: ActorRef, actorId: Long, actorRef: ActorRef): ActorRef =
    Await.result(
      router
        .ask(Register(actorId, actorRef))(defaultTimeout)
        .mapTo[RegisterResponse]
        .map {
          case RegisterOk(_) =>
            actorRef
          case _ =>
            throw new Exception("Failed to register node")
        },
      Duration.Inf
    )

  // Function to create a new CheckPredecessorAlgorithm actor using a given ActorRef as the Pointers actor
  private def newAlgorithmActor(router: ActorRef, pointersActor: ActorRef): ActorRef =
    system.actorOf(CheckPredecessorAlgorithm.props(router, pointersActor, defaultTimeout))

  // Actor that will always discard messages
  private val unresponsiveActor = TestActorRef(new Actor {
    override def receive: Receive = {
      case _ =>
    }
  })

  "A CheckPredecessorAlgorithm actor" when {
    "initialised with a Pointers actor that does not have a predecessor" should {

      // Fake Pointers actor that responds with GetPredecessorOkButUnknown when GetPredecessor is received
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
        val router = newRouter
        val pointers = newPointersActor
        val algorithm = newAlgorithmActor(router, pointers)

        algorithm ! CheckPredecessorAlgorithmStart
        expectMsg(CheckPredecessorAlgorithmFinished)
        expectNoMessage(spuriousMessageDuration)
      }
    }

    "initialised with a Pointers actor whose predecessor is healthy" should {

      // Fake Pointers actor that responds with GetPredecessorOk when GetPredecessor is received
      def newPointersActor(router: ActorRef): ActorRef = {
        val healthyPredecessor = registerNode(router, 1L, TestActorRef(new Actor {
          override def receive: Receive = {
            case GetSuccessor =>
              sender() ! GetSuccessorOk(NodeInfo(1L, dummyAddr, self))
          }
        }))

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
        val router = newRouter
        val pointers = newPointersActor(router)
        val algorithm = newAlgorithmActor(router, pointers)

        algorithm ! CheckPredecessorAlgorithmStart
        expectMsg(CheckPredecessorAlgorithmFinished)
        expectNoMessage(spuriousMessageDuration)
      }
    }

    "initialised with a Pointers actor whose predecessor is unresponsive" should {

      // Fake Pointers actor that responds with GetPredecessorOk when GetPredecessor is received, with a reference
      // to a unresponsive Node. After receiving ResetPredecessor, it will respond with GetPredecessorOkButUnknown
      // instead, having 'forgotten' its predecessor.
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
        val router = newRouter
        val algorithm = newAlgorithmActor(router, pointersActor)

        algorithm ! CheckPredecessorAlgorithmStart
        expectMsg(CheckPredecessorAlgorithmFinished)
        expectNoMessage(spuriousMessageDuration)
        pointersActor ! GetPredecessor
        expectMsg(GetPredecessorOkButUnknown)
      }
    }

    "initialised with a Pointers actor that is unresponsive" should {
      "finish with an error" in {
        val router = newRouter
        val algorithm = newAlgorithmActor(router, unresponsiveActor)

        algorithm ! CheckPredecessorAlgorithmStart
        expectMsgType[CheckPredecessorAlgorithmError]
        expectNoMessage(spuriousMessageDuration)
      }
    }
  }
}
