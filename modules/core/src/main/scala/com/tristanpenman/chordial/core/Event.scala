package com.tristanpenman.chordial.core

sealed trait Event

object Event {

  case class FingerReset(nodeId: Long, index: Int) extends Event

  case class FingerUpdated(nodeId: Long, index: Int, fingerId: Long) extends Event

  case class NodeCreated(nodeId: Long, successorId: Long) extends Event

  case class NodeShuttingDown(nodeId: Long) extends Event

  case class PredecessorReset(nodeId: Long) extends Event

  case class PredecessorUpdated(nodeId: Long, predecessorId: Long) extends Event

  case class SuccessorUpdated(nodeId: Long, successorId: Long) extends Event

}
