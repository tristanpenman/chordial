package com.tristanpenman.chordial.core

import akka.actor._

object NodeProtocol {
  case class GetSuccessor()
  case class GetSuccessorOk(successorId: Long, successorRef: ActorRef)
  case class GetSuccessorError(message: String)

  case class Hello()
  case class HelloResponse()

  case class Join(seed: Option[ActorRef])
  case class JoinOk()
  case class JoinError(message: String)
}

class Node(ownId: Long) extends Actor with ActorLogging {
  import NodeProtocol._

  private def receiveWhileReady(successorId: Long, successorRef: ActorRef): Receive = {
    case GetSuccessor =>
      sender() ! GetSuccessorOk(successorId, successorRef)
    case Hello =>
      sender() ! HelloResponse
    case Join(seed) =>
      sender() ! JoinError("Not implemented")
  }

  override def receive = receiveWhileReady(ownId, self)
}
