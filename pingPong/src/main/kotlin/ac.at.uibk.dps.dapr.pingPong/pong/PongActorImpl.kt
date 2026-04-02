package ac.at.uibk.dps.dapr.pingPong.pong

import ac.at.uibk.dps.dapr.pingPong.PingPong
import io.dapr.actors.ActorId
import io.dapr.actors.runtime.AbstractActor
import io.dapr.actors.runtime.ActorRuntimeContext
import io.dapr.client.DaprClient
import io.dapr.client.DaprClientBuilder
import java.util.concurrent.TimeUnit
import kotlin.time.Clock

class PongActorImpl(runtimeContext: ActorRuntimeContext<PongActorImpl>, actorId: ActorId) :
  AbstractActor(runtimeContext, actorId), PongActor {
  val client: DaprClient = DaprClientBuilder().build()

  var metricRegistry = PingPong.provideMetricRegistry()

  override fun pong(time: Long) {
    val now = Clock.System.now()
    val nowNanos = (now.epochSeconds * 1_000_000_000L) + now.nanosecondsOfSecond
    val deltaNanos = (nowNanos - time).coerceAtLeast(0L)

    metricRegistry.timer("event.latency").update((deltaNanos), TimeUnit.NANOSECONDS)

    client.publishEvent("pubsub", "pong", nowNanos).subscribe()
  }
}
