package ac.at.uibk.dps.dapr.cigarette.arbiter

import io.dapr.actors.ActorId
import io.dapr.actors.runtime.AbstractActor
import io.dapr.actors.runtime.ActorRuntimeContext
import io.dapr.client.DaprClient
import io.dapr.client.DaprClientBuilder

class ArbiterActorImpl(runtimeContext: ActorRuntimeContext<ArbiterActorImpl>, actorId: ActorId) :
  AbstractActor(runtimeContext, actorId), ArbiterActor {

  val client: DaprClient = DaprClientBuilder().build()
  val ingredients = listOf("smoker-0", "smoker-1", "smoker-2")
  var count = 0
  var start = 0L

  override fun provide() {
    if (count == 0) {
      start = System.nanoTime()
    }
    count++
    val provide = ingredients.toMutableList()
    provide.remove(provide.random())
    client.publishEvent("pubsub", "provide", provide).subscribe()
  }
}
