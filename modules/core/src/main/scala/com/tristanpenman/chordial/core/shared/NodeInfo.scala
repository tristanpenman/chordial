package com.tristanpenman.chordial.core.shared

import akka.actor.ActorRef

final case class NodeInfo(id: Long, ref: ActorRef)
