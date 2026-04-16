package ac.at.uibk.dps.dapr.philosophers.instantiator

import ac.at.uibk.dps.dapr.philosophers.ChandyMisra
import io.dapr.actors.ActorId
import io.dapr.actors.runtime.AbstractActor
import io.dapr.actors.runtime.ActorRuntimeContext
import io.dapr.client.DaprClient
import io.dapr.client.DaprClientBuilder

class InstantiatorActorImpl(
  runtimeContext: ActorRuntimeContext<InstantiatorActorImpl>,
  id: ActorId,
) : AbstractActor(runtimeContext, id), InstantiatorActor {
  private val client: DaprClient = DaprClientBuilder().build()

  val metricsRegistry = ChandyMisra.provideMetricRegistry()
}
