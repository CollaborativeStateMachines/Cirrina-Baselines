package ac.at.uibk.dps.dapr.big.big

import io.dapr.actors.ActorId
import io.dapr.actors.runtime.AbstractActor
import io.dapr.actors.runtime.ActorRuntimeContext
import io.dapr.client.DaprClientBuilder

class BigActorImpl(runtimeContext: ActorRuntimeContext<BigActorImpl>, val actorId: ActorId) :
  AbstractActor(runtimeContext, actorId), BigActor {

  val client = DaprClientBuilder().build()
  var neighbors = emptyList<String>()
  var target: String = ""
  var count = 0

  override fun register() {
    client.publishEvent("pubsub", "register", actorId.toString()).subscribe()
    println("$actorId sent register")
  }

  override fun assignNeighbors(neighbors: List<String>) {
    this.neighbors = neighbors
    sendPing()
  }

  override fun receivePong(sender: String) {
    if (sender == target) {
      sendPing()
      count++
    } else {
      println("$actorId received pong from $sender, expected $target")
      return
    }
  }

  override fun sendPing() {
    target = neighbors.random()
    client
      .publishEvent("pubsub", "ping", mapOf("sender" to actorId.toString(), "receiver" to target))
      .subscribe()
  }

  override fun sendPong(sender: String) {
    client
      .publishEvent("pubsub", "pong", mapOf("sender" to actorId.toString(), "receiver" to sender))
      .subscribe()
  }
}
