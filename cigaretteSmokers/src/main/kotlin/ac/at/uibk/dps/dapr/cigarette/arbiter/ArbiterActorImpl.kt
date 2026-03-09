package ac.at.uibk.dps.dapr.cigarette.arbiter

import io.dapr.actors.ActorId
import io.dapr.actors.runtime.AbstractActor
import io.dapr.actors.runtime.ActorRuntimeContext
import io.dapr.client.DaprClient
import io.dapr.client.DaprClientBuilder
import io.micrometer.core.instrument.Metrics

class ArbiterActorImpl(
  runtimeContext: ActorRuntimeContext<ArbiterActorImpl>,
  actorId: ActorId,
) : AbstractActor(runtimeContext, actorId), ArbiterActor {

  val client: DaprClient = DaprClientBuilder().build()
  val ingredients = listOf("smoker-0", "smoker-1", "smoker-2")
  var counter = Metrics.counter("arbiter.rounds")
  var count = 0
  var start = 0L

  override fun provide() {
    if (count == 0) {
      start = System.nanoTime()
    }
    if (count == 20000) { done(); return }
    count++
    counter.increment()
    val provide = ingredients.toMutableList()
    provide.remove(provide.random())
    client.publishEvent("pubsub", "provide", provide).subscribe()
  }

  override fun done() {
    println("Finished $count rounds of smoking in ${(System.nanoTime() - start)/ 1_000_000} ms")
  }
}
