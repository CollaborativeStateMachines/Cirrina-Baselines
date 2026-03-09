package ac.at.uibk.dps.dapr.count.producer

import io.dapr.actors.ActorId
import io.dapr.actors.runtime.AbstractActor
import io.dapr.actors.runtime.ActorRuntimeContext
import io.dapr.client.DaprClient
import io.dapr.client.DaprClientBuilder
import reactor.core.publisher.Flux

class ProducerActorImpl(runtimeContext: ActorRuntimeContext<ProducerActorImpl>, actorId: ActorId) :
  AbstractActor(runtimeContext, actorId), ProducerActor {
  val client: DaprClient = DaprClientBuilder().build()
  var startTime = 0L

  override fun produce() {
    startTime = System.nanoTime()
    Flux.range(1, 50000)
      .flatMap({ client.publishEvent("pubsub", "increment", startTime) }, 100)
      .subscribe()
  }
}
