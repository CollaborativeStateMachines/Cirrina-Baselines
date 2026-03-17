package ac.at.uibk.dps.dapr.cigarette.smoker

import io.dapr.actors.ActorId
import io.dapr.actors.runtime.AbstractActor
import io.dapr.actors.runtime.ActorRuntimeContext
import io.dapr.client.DaprClient
import io.dapr.client.DaprClientBuilder

class SmokerActorImpl(runtimeContext: ActorRuntimeContext<SmokerActorImpl>, val actorId: ActorId) :
    AbstractActor(runtimeContext, actorId), SmokerActor {

  val client: DaprClient = DaprClientBuilder().build()

  override fun smoke(ingredients: List<String>) {
    if (!ingredients.contains(actorId.toString())) {
      Thread.sleep(0)
      client.publishEvent("pubsub", "done", 0L).subscribe()
    }
  }
}
