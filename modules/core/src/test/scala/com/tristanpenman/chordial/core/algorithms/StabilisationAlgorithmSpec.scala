package com.tristanpenman.chordial.core.algorithms

import java.net.InetSocketAddress

import akka.actor.{Actor, ActorRef, ActorSystem}
import akka.testkit.{ImplicitSender, TestActorRef, TestKit}
import akka.util.Timeout
import com.tristanpenman.chordial.core.Node.{Notify, NotifyOk}
import com.tristanpenman.chordial.core.Pointers
import com.tristanpenman.chordial.core.Pointers._
import com.tristanpenman.chordial.core.algorithms.StabilisationAlgorithm._
import com.tristanpenman.chordial.core.shared.NodeInfo
import org.scalatest.WordSpecLike

import scala.concurrent.duration._

final class StabilisationAlgorithmSpec
    extends TestKit(ActorSystem("StabilisationAlgorithmSpec"))
    with WordSpecLike
    with ImplicitSender {

  private val keyspaceBits = 3

  // Timeout for requests performed within CheckPredecessorAlgorithm actor
  private val algorithmTimeout = Timeout(300.milliseconds)

  // Time to wait before concluding that no additional messages will be received
  private val spuriousMessageDuration = 150.milliseconds

  private val dummyAddr = new InetSocketAddress("0.0.0.0", 0)

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
          StabilisationAlgorithm.props(NodeInfo(1L, dummyAddr, nodeRef), pointersRef, algorithmTimeout)
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
