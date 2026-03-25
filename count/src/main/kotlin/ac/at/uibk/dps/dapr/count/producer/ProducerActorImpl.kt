package ac.at.uibk.dps.dapr.count.producer

import ac.at.uibk.dps.dapr.count.Count
import io.dapr.actors.ActorId
import io.dapr.actors.runtime.AbstractActor
import io.dapr.actors.runtime.ActorRuntimeContext
import io.dapr.client.DaprClient
import io.dapr.client.DaprClientBuilder
import kotlin.time.Clock
import kotlin.time.measureTime
import kotlin.time.toJavaDuration

class ProducerActorImpl(runtimeContext: ActorRuntimeContext<ProducerActorImpl>, actorId: ActorId) :
  AbstractActor(runtimeContext, actorId), ProducerActor {
  val client: DaprClient = DaprClientBuilder().build()

  var metricRegistry = Count.provideMetricRegistry()
  var produceTimer = metricRegistry.timer("produce.duration")

  override fun produce() {
    while (true) {
      val delta = measureTime {
        val now = Clock.System.now()
        val epochNanos = (now.epochSeconds * 1_000_000_000L) + now.nanosecondsOfSecond

        client.publishEvent("pubsub", "increment", epochNanos).subscribe()

        Thread.sleep(10)
      }
      produceTimer.update(delta.toJavaDuration())
    }
  }
}
