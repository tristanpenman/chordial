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

  IO(Udp) ! Udp.SimpleSender

  private def ready(nodes: Map[Long, ActorRef], udpSend: ActorRef): Receive = {
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

  override def receive: Receive = {
    case Udp.SimpleSenderReady =>
      context.become(ready(initialNodes, sender()))
      unstashAll()
    case Udp.CommandFailed =>
      log.error("bind failed")
      context.stop(self)
    case _ => stash()
  }
}

object Router {
  sealed trait RouterRequest

  final case class Forward(id: Long, addr: InetSocketAddress, message: Object) extends RouterRequest

  final case class Register(id: Long, ref: ActorRef) extends RouterRequest

  final case class Unregister(id: Long) extends RouterRequest

  sealed trait RouterResponse

  final case class RegisterFailed(id: Long, reason: String) extends RouterResponse

  final case class RegisterOk(id: Long) extends RouterResponse

  final case class UnregisterFailed(id: Long, reason: String) extends RouterResponse

  final case class UnregisterOk(id: Long) extends RouterResponse

  def props(initialNodes: Map[Long, ActorRef] = Map.empty): Props = Props(new Router(initialNodes))
}
