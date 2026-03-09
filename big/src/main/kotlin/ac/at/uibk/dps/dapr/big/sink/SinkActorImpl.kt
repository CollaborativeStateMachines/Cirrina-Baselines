package ac.at.uibk.dps.dapr.big.sink

import io.dapr.actors.ActorId
import io.dapr.actors.runtime.AbstractActor
import io.dapr.actors.runtime.ActorRuntimeContext
import io.dapr.client.DaprClientBuilder

class SinkActorImpl(runtimeContext: ActorRuntimeContext<SinkActorImpl>, val actorId: ActorId) :
  AbstractActor(runtimeContext, actorId), SinkActor {

  val client = DaprClientBuilder().build()
  var bigs = mutableListOf<String>()
  var startTime = 0L

  override fun register(actor: String) {
    if (!bigs.contains(actor)) {
      bigs.add(actor)
      println("Register actor $actor")
    } else {
      println("Big $actor already registered!")
    }
    if (bigs.size == 12) {
      println("Received all bigs")
      sendNeighbors()
    }
  }

  override fun sendNeighbors() {
    startTime = System.nanoTime()
    client.publishEvent("pubsub", "neighbors", bigs.toList()).subscribe()
    bigs.clear()
  }

  override fun receiveDone(sender: String) {
    if (!bigs.contains(sender)) {
      bigs.add(sender)
    } else {
      println("Big $sender already signaled done!")
    }
    if (bigs.size == 12) {
      println("All bigs finished in ${(System.nanoTime() - startTime)/ 1_000_000} ms")
    }
  }
}
