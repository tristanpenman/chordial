package com.tristanpenman.chordial.daemon.service

import akka.actor.{Actor, ActorContext, ActorRef}

class ServiceActor(val nodeRef: ActorRef) extends Actor with Service {
  def actorRefFactory: ActorContext = context

  override def receive: Receive = runRoute(routes)
}
