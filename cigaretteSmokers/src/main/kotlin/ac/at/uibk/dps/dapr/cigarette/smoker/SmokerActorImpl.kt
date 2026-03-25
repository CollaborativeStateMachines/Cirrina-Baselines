package ac.at.uibk.dps.dapr.cigarette.smoker

import ac.at.uibk.dps.dapr.cigarette.CigaretteSmokers
import io.dapr.actors.ActorId
import io.dapr.actors.runtime.AbstractActor
import io.dapr.actors.runtime.ActorRuntimeContext
import io.dapr.client.DaprClient
import io.dapr.client.DaprClientBuilder
import java.util.concurrent.TimeUnit
import kotlin.time.Clock
import kotlin.time.measureTime
import kotlin.time.toJavaDuration

class SmokerActorImpl(runtimeContext: ActorRuntimeContext<SmokerActorImpl>, val actorId: ActorId) :
  AbstractActor(runtimeContext, actorId), SmokerActor {

  val client: DaprClient = DaprClientBuilder().build()

  var metricRegistry = CigaretteSmokers.provideMetricRegistry()
  var eventTimer = metricRegistry.timer("event.latency")
  var smokeTimer = metricRegistry.timer("smoke.duration")

  override fun smoke(data: Map<String, Any>) {
    val delta = measureTime {
      val ingredients = data["ingredients"] as List<String>
      val time = data["time"] as Long

      if (!ingredients.contains(actorId.toString())) {
        var now = Clock.System.now()
        val nowNanos = (now.epochSeconds * 1_000_000_000L) + now.nanosecondsOfSecond

        val deltaNanos = (nowNanos - time).coerceAtLeast(0L)

        eventTimer.update(deltaNanos, TimeUnit.NANOSECONDS)

        Thread.sleep(10)

        now = Clock.System.now()
        val epochNanos = (now.epochSeconds * 1_000_000_000L) + now.nanosecondsOfSecond

        client.publishEvent("pubsub", "done", epochNanos).subscribe()
      }
    }
    smokeTimer.update(delta.toJavaDuration())
  }
}
