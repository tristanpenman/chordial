package com.tristanpenman.chordial.core

import java.net.InetSocketAddress

import akka.actor.{Actor, ActorLogging, ActorRef, Props, Stash}
import akka.io.{IO, Udp}
import akka.serialization.{Serialization, SerializationExtension}
import akka.util.ByteString

/**
  * Actor class that routes messages to local or remote nodes
  *
  * Given the ID of the intended recipient, the Router is responsible for deciding whether to send that message
  * directly to a local Node actor, or to a remote Node actor via UDP.
  */
class Router(initialNodes: Map[Long, ActorRef]) extends Actor with ActorLogging with Stash {
  import Router._
  import context.system

  val serialization: Serialization = SerializationExtension(system)

  private def ready(nodes: Map[Long, ActorRef], udpSend: ActorRef): Receive = {
    case Start(_, _) =>
      sender() ! StartFailed("aready started")

    case Forward(id, addr, message) =>
      nodes.get(id) match {
        case Some(ref) =>
          ref ! message
        case _ =>
          val s = serialization.findSerializerFor(message)
          val b: Array[Byte] = s.toBinary(message)
          udpSend ! Udp.Send(ByteString(b), addr)
      }

    case Register(id, ref) =>
      if (nodes.contains(id)) {
        sender() ! RegisterFailed(id, "already registered")
        log.debug("already registered")
      } else {
        context.become(ready(nodes + (id -> ref), udpSend))
        sender() ! RegisterOk(id)
      }

    case Unregister(id) =>
      if (nodes.contains(id)) {
        context.become(ready(nodes - id, udpSend))
        sender() ! UnregisterOk(id)
      } else {
        sender() ! UnregisterFailed(id, "not registered")
      }

    case _ =>
      log.error("unexpected message")
  }

  def starting(replyTo: ActorRef): Receive = {
    case Start(_, _) =>
      sender() ! StartFailed("already starting")

    case Udp.Bound(localAddress) =>
      context.become(ready(initialNodes, sender()))
      replyTo ! StartOk(localAddress)
      unstashAll()

    case Udp.CommandFailed(_) =>
      log.error("bind failed; committing seppuku")
      context.stop(self)

    case m =>
      log.debug("received unexpected message", m)
      stash()
  }

  override def receive: Receive = {
    case Start(nodeAddress, nodePort) =>
      context.become(starting(sender()))
      IO(Udp) ! Udp.Bind(self, new InetSocketAddress(nodeAddress, nodePort))

    case _ => stash()
  }
}

object Router {
  sealed trait RouterRequest

  final case class Forward(id: Long, addr: InetSocketAddress, message: Object) extends RouterRequest

  final case class Register(id: Long, ref: ActorRef) extends RouterRequest

  final case class Start(nodeAddress: String, nodePort: Int) extends RouterRequest

  final case class Unregister(id: Long) extends RouterRequest

  sealed trait RegisterResponse

  final case class RegisterFailed(id: Long, reason: String) extends RegisterResponse

  final case class RegisterOk(id: Long) extends RegisterResponse

  sealed trait StartResponse

  final case class StartFailed(reason: String) extends StartResponse

  final case class StartOk(localAddress: InetSocketAddress) extends StartResponse

  sealed trait UnregisterResponse

  final case class UnregisterFailed(id: Long, reason: String) extends UnregisterResponse

  final case class UnregisterOk(id: Long) extends UnregisterResponse

  def props(initialNodes: Map[Long, ActorRef] = Map.empty): Props = Props(new Router(initialNodes))
}
