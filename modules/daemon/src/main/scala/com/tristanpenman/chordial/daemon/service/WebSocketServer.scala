package com.tristanpenman.chordial.daemon.service

import akka.actor.{Props, ActorRef, Actor}
import spray.can.Http

class WebSocketServer(val nodeRef: ActorRef) extends Actor {
  override def receive: Receive = {
    case Http.Connected(remoteAddress, localAddress) =>
      val serverConnection = sender()
      val conn = context.actorOf(WebSocketWorker.props(serverConnection, nodeRef))
      serverConnection ! Http.Register(conn)
  }
}

object WebSocketServer {
  def props(nodeRef: ActorRef): Props = Props(new WebSocketServer(nodeRef))
}