package com.tristanpenman.chordial.daemon.service

import akka.actor.{Actor, ActorRef}

class ServiceActor(val nodeId: Long, val nodeRef: ActorRef) extends Actor with Service {
  def actorRefFactory = context

  protected def id = nodeId
  protected def ref = nodeRef

  override def receive = runRoute(routes)
}
