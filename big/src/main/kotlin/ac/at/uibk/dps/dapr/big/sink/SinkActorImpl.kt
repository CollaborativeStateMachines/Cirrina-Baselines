package ac.at.uibk.dps.dapr.big.sink

import ac.at.uibk.dps.dapr.big.Big
import io.dapr.actors.ActorId
import io.dapr.actors.runtime.AbstractActor
import io.dapr.actors.runtime.ActorRuntimeContext
import io.dapr.client.DaprClientBuilder

class SinkActorImpl(runtimeContext: ActorRuntimeContext<SinkActorImpl>, actorId: ActorId) :
  AbstractActor(runtimeContext, actorId), SinkActor {
  val client = DaprClientBuilder().build()
  var metricRegistry = Big.provideMetricRegistry()

  private var total = 0
  private var registered = 0

  override fun register() {
    ++registered
    if (registered == 12) {
      client.publishEvent("pubsub", "initial", 0L).subscribe()
    }
  }

  override fun report(data: Map<String, Any>) {
    total += 1000
    metricRegistry.counter("sink.total").inc(1000L)
  }
}
