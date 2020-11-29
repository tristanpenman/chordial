package com.tristanpenman.chordial.core.shared

import java.net.InetSocketAddress

import akka.actor.ActorRef

final case class NodeInfo(id: Long, addr: InetSocketAddress, ref: ActorRef)
