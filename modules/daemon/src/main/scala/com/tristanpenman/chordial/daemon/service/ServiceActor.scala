package com.tristanpenman.chordial.daemon.service

import akka.actor.{Actor, ActorRef}

class ServiceActor(val nodeRef: ActorRef) extends Actor with Service {
  def actorRefFactory = context

  override def receive = runRoute(routes)
}
