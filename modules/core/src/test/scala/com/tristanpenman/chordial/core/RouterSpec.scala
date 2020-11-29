package com.tristanpenman.chordial.core

import java.net.InetSocketAddress

import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import akka.io.{IO, Udp}
import akka.pattern.ask
import akka.serialization.{Serialization, SerializationExtension}
import akka.testkit.{ImplicitSender, TestKit, TestProbe}
import akka.util.{ByteString, Timeout}
import org.scalatest.WordSpecLike

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContextExecutor}

class RouterSpec extends TestKit(ActorSystem("RouterSpec")) with WordSpecLike with ImplicitSender {
  import Router._

  implicit val ec: ExecutionContextExecutor = system.dispatcher

  private val testProbe1 = TestProbe()
  private val testProbe2 = TestProbe()

  private val defaultTimeout = Timeout(1.second)

  private def startRouter(initialNodes: Map[Long, ActorRef]) = {
    val routerRef = system.actorOf(Router.props(initialNodes))

    val future = routerRef
      .ask(Start("0.0.0.0", 0))(defaultTimeout)
      .mapTo[StartResponse]
      .map {
        case StartOk(_) =>
          routerRef
        case StartFailed(reason) =>
          throw new Exception(s"Failed to start Router: ${reason}")
      }

    Await.result(future, Duration.Inf)
  }

  "A RouterSpec actor" when {
    "initially constructed" should {
      "respond to a Register message with a RegisterOk message, then RegisterFailed for subsequent attempts" in {
        val routerRef = startRouter(Map.empty)

        routerRef ! Register(1, testProbe1.ref)
        expectMsg(RegisterOk(1))

        routerRef ! Register(1, testProbe1.ref)
        expectMsg(RegisterFailed(1, "already registered"))
      }

      "respond to a Unregister message with UnregisterFailed message if node is not registered" in {
        val routerRef = startRouter(Map.empty)

        routerRef ! Unregister(1)
        expectMsg(UnregisterFailed(1, "not registered"))
      }
    }

    "one node is registered" should {
      "respond to a Register message with a RegisterFailed message if id is already registered" in {
        val routerRef = startRouter(Map(1L -> testProbe1.ref))

        routerRef ! Register(1, testProbe1.ref)
        expectMsg(RegisterFailed(1, "already registered"))
      }

      "respond to a Register message with a RegisterOk message if id is not already registered" in {
        val routerRef = startRouter(Map(1L -> testProbe1.ref))

        routerRef ! Register(2, testProbe2.ref)
        expectMsg(RegisterOk(2))
      }

      "respond to a Unregister message with an UnregisterOk message if node is registered" in {
        val routerRef = startRouter(Map(1L -> testProbe1.ref))

        routerRef ! Unregister(1)
        expectMsg(UnregisterOk(1))

        routerRef ! Unregister(1)
        expectMsg(UnregisterFailed(1, "not registered"))
      }
    }

    "two local nodes are registered" should {
      "handle a Forward message for a registered node by forwarding it to that node" in {
        val routerRef = startRouter(Map(1L -> testProbe1.ref, 2L -> testProbe2.ref))

        routerRef ! Forward(2L, new InetSocketAddress("localhost", 333), "message")
        testProbe2.expectMsg("message")

        // message should not be sent to the other node
        testProbe1.expectNoMessage(0.millis)
      }

      "handle a Forward message for a remote node by sending the message via UDP" in {
        val routerRef = startRouter(Map(1L -> testProbe1.ref, 2L -> testProbe2.ref))
        val udpReceiverProbe = TestProbe()

        // create an actor that can receive UDP messages
        val udpReceiverRef = system.actorOf(Props(new Actor {
          val serialization: Serialization = SerializationExtension(system)

          def receive: Receive = {
            case "bind" =>
              IO(Udp) ! Udp.Bind(self, new InetSocketAddress("localhost", 0))
              context.become(binding)
          }

          def binding: Receive = {
            case Udp.Bound(localAddress) =>
              context.become(ready(sender()))
              routerRef ! Forward(3L, localAddress, "message")
          }

          def ready(socket: ActorRef): Receive = {
            case Udp.Received(data: ByteString, _: InetSocketAddress) =>
              val s = serialization.findSerializerFor("message")
              val v = s.fromBinary(data.toArray)
              udpReceiverProbe.ref ! v
              self ! Udp.Unbind
            case Udp.Unbind  => socket ! Udp.Unbind
            case Udp.Unbound => context.stop(self)
          }
        }))

        // wait for the message to be received and for the actor to terminate
        udpReceiverProbe.watch(udpReceiverRef)
        udpReceiverRef ! "bind"
        udpReceiverProbe.expectMsg("message")
        udpReceiverProbe.expectTerminated(udpReceiverRef)

        // message should not be sent to the registered nodes
        testProbe1.expectNoMessage(0.millis)
        testProbe2.expectNoMessage(0.millis)
      }
    }
  }
}
