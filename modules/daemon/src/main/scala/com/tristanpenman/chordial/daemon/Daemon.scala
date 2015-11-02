package com.tristanpenman.chordial.daemon

import akka.actor.{Props, Actor, ActorLogging}

import com.tristanpenman.chordial.core.Node
import com.tristanpenman.chordial.core.NodeProtocol._

class Daemon extends Actor with ActorLogging {
  private val node = context.actorOf(Props(classOf[Node], 0L))

  node ! Hello

  override def receive: Receive = {
    case HelloResponse =>
      log.info("Node responded to Hello");
  }
}

object Daemon {
  def main(args: Array[String]): Unit = {
    akka.Main.main(Array(classOf[Daemon].getName))
  }
}
