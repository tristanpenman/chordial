package com.tristanpenman.chordial.dht

import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestKit}
import org.scalatest.wordspec.AnyWordSpecLike

import scala.concurrent.duration.DurationInt

final class CoordinatorSpec extends TestKit(ActorSystem("CoordinatorSpec")) with AnyWordSpecLike with ImplicitSender {

  // Time to wait before concluding that no additional messages will be received
  private val spuriousMessageDuration = 150.milliseconds

  // Limit size of keyspace / number of nodes to 2^3 == 8
  private val keyspaceBits = 3

  "A Coordinator actor" when {
    "initially constructed" should {
      def coordinator = system.actorOf(Coordinator.props(keyspaceBits, "0.0.0.0", 0, None))

      "not crash when it receives messages" in {
        coordinator ! "Test"
        expectNoMessage(spuriousMessageDuration)
      }
    }
  }
}
