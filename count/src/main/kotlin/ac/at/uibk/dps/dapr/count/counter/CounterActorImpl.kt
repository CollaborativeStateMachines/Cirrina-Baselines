package ac.at.uibk.dps.dapr.count.counter

import io.dapr.actors.ActorId
import io.dapr.actors.runtime.AbstractActor
import io.dapr.actors.runtime.ActorRuntimeContext
import io.dapr.client.DaprClientBuilder

class CounterActorImpl(runtimeContext: ActorRuntimeContext<CounterActorImpl>, actorId: ActorId) :
  AbstractActor(runtimeContext, actorId), CounterActor {

  val client = DaprClientBuilder().build()
  var count = 0
  var startTime: Long = 0L

  override fun increment(time: Long) {
    if (count == 0) {
      startTime = time
    }
    count += 1
    if (count == 50000) {
      println("$count messages in ${(System.nanoTime() - startTime)/ 1_000_000} ms")
      client.publishEvent("pubsub", "done", count).subscribe()
    }
  }
}
