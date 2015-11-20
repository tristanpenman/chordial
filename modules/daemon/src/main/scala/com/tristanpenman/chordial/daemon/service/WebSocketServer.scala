package com.tristanpenman.chordial.daemon.service

import akka.actor.{Actor, ActorRef, Props}
import com.tristanpenman.chordial.core.Event
import spray.can.Http

class WebSocketServer(val nodeRef: ActorRef) extends Actor {
  override def receive: Receive = {
    case Http.Connected(remoteAddress, localAddress) =>
      val serverConnection = sender()
      val conn = context.actorOf(WebSocketWorker.props(serverConnection, nodeRef))
      serverConnection ! Http.Register(conn)

    case e: Event =>
      context.children.foreach { _ ! e }
  }
}

object WebSocketServer {
  def props(nodeRef: ActorRef): Props = Props(new WebSocketServer(nodeRef))
}
