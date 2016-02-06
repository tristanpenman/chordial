package com.tristanpenman.chordial.core.algorithms

import akka.actor.{Actor, ActorSystem}
import akka.testkit.{ImplicitSender, TestActorRef, TestKit}
import akka.util.Timeout
import com.tristanpenman.chordial.core.Node.{Notify, NotifyOk}
import com.tristanpenman.chordial.core.Pointers
import com.tristanpenman.chordial.core.Pointers.{GetPredecessor, GetPredecessorOk, GetSuccessorList, GetSuccessorListOk}
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

  val algorithm = system.actorOf(
    StabilisationAlgorithm.props(NodeInfo(1L, nodeRef), pointersRef, algorithmTimeout))

  "A StabilisationAlgorithm actor" when {
    "initialised with a Pointers actor for a node that is its own successor" should {
      "finish successfully without sending any further messages" in {
        algorithm ! StabilisationAlgorithmStart()
        expectMsg(StabilisationAlgorithmFinished())
        expectNoMsg(spuriousMessageDuration)
      }
    }
  }
}
