package com.tristanpenman.chordial.core

import akka.actor._

object NodeProtocol {
  case class Hello()
  case class HelloResponse()
}

class Node extends Actor with ActorLogging {
  import NodeProtocol._

  override def receive = {
    case Hello =>
      sender() ! HelloResponse

    case message =>
      log.warning("Received unexpected {} message", message.getClass.getSimpleName)
  }
}
