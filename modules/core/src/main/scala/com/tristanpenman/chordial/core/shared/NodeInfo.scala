package com.tristanpenman.chordial.core.shared

import akka.actor.ActorRef

case class NodeInfo(id: Long, ref: ActorRef)
