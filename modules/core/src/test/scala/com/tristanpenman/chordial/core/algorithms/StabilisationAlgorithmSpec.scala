package com.tristanpenman.chordial.core.algorithms

import java.net.InetSocketAddress

import akka.actor.{Actor, ActorRef, ActorSystem}
import akka.pattern.ask
import akka.testkit.{ImplicitSender, TestActorRef, TestKit}
import akka.util.Timeout
import com.tristanpenman.chordial.core.Node.{Notify, NotifyOk}
import com.tristanpenman.chordial.core.{Pointers, Router}
import com.tristanpenman.chordial.core.Pointers._
import com.tristanpenman.chordial.core.Router.{Start, StartFailed, StartOk, StartResponse}
import com.tristanpenman.chordial.core.algorithms.StabilisationAlgorithm._
import com.tristanpenman.chordial.core.shared.NodeInfo
import org.scalatest.WordSpecLike

import scala.concurrent.{Await, ExecutionContextExecutor}
import scala.concurrent.duration._

final class StabilisationAlgorithmSpec
    extends TestKit(ActorSystem("StabilisationAlgorithmSpec"))
    with WordSpecLike
    with ImplicitSender {

  implicit val ec: ExecutionContextExecutor = system.dispatcher

  private val keyspaceBits = 3

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

  "A StabilisationAlgorithm actor" when {
    "initialised with a Pointers actor for a node that is its own successor" should {
      def newAlgorithm: ActorRef = {
        val nodeRef = TestActorRef(new Actor {
          override def receive: Receive = {
            case GetPredecessor =>
              sender() ! GetPredecessorOk(NodeInfo(1L, dummyAddr, self))
            case GetSuccessor =>
              sender() ! GetSuccessorOk(NodeInfo(1L, dummyAddr, self))
            case Notify(_, _, _) =>
              sender() ! NotifyOk
            case m =>
              fail(s"Stub Node actor received an unexpected message of type: ${m.getClass})")
          }
        })

        val pointersRef = system.actorOf(
          Pointers.props(1L, keyspaceBits, NodeInfo(1L, dummyAddr, nodeRef), system.eventStream)
        )

        system.actorOf(
          StabilisationAlgorithm.props(router, NodeInfo(1L, dummyAddr, nodeRef), pointersRef, defaultTimeout)
        )
      }

      "finish successfully without sending any further messages" in {
        newAlgorithm ! StabilisationAlgorithmStart
        expectMsg(StabilisationAlgorithmFinished)
        expectNoMessage(spuriousMessageDuration)
      }
    }
  }
}
