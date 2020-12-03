package com.tristanpenman.chordial.core.algorithms

import java.net.InetSocketAddress
import akka.actor.{Actor, ActorRef, ActorSystem}
import akka.pattern.ask
import akka.testkit.{ImplicitSender, TestActorRef, TestKit}
import akka.util.Timeout
import com.tristanpenman.chordial.core.Node.{Notify, NotifyOk}
import com.tristanpenman.chordial.core.{Pointers, Router}
import com.tristanpenman.chordial.core.Pointers._
import com.tristanpenman.chordial.core.Router.{
  Register,
  RegisterOk,
  RegisterResponse,
  Start,
  StartFailed,
  StartOk,
  StartResponse
}
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

  private val dummyInfo = NodeInfo(1L, dummyAddr, self)

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

  def newAlgorithm(router: ActorRef, pointers: ActorRef): ActorRef = {
    registerNode(
      router,
      1L,
      TestActorRef(new Actor {
        override def receive: Receive = {
          case GetPredecessor =>
            sender() ! GetPredecessorOk(dummyInfo)
          case GetSuccessor =>
            sender() ! GetSuccessorOk(dummyInfo)
          case Notify(_, _, _) =>
            sender() ! NotifyOk
          case m =>
            fail(s"Stub Node actor received an unexpected message of type: ${m.getClass})")
        }
      })
    )

    system.actorOf(StabilisationAlgorithm.props(router, dummyInfo, pointers, defaultTimeout))
  }

  "A StabilisationAlgorithm actor" when {
    "initialised with a node that is its own successor" should {
      "finish successfully without sending any further messages" in {
        val router = newRouter
        val pointers = system.actorOf(Pointers.props(1L, keyspaceBits, dummyInfo, system.eventStream))
        val algorithm = newAlgorithm(router, pointers)

        algorithm ! StabilisationAlgorithmStart
        expectMsg(StabilisationAlgorithmFinished)
        expectNoMessage(spuriousMessageDuration)
      }
    }
  }
}
