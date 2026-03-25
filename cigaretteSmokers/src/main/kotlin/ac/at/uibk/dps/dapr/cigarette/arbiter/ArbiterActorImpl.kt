package ac.at.uibk.dps.dapr.cigarette.arbiter

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

class ArbiterActorImpl(runtimeContext: ActorRuntimeContext<ArbiterActorImpl>, actorId: ActorId) :
  AbstractActor(runtimeContext, actorId), ArbiterActor {

  val client: DaprClient = DaprClientBuilder().build()

  val ingredients = listOf("0", "1", "2")
  var count = 0

  var metricRegistry = CigaretteSmokers.provideMetricRegistry()
  var counter = metricRegistry.counter("arbiter.rounds")
  var eventTimer = metricRegistry.timer("event.latency")
  var provideTimer = metricRegistry.timer("provide.duration")

  override fun provide(time: Long) {
    val delta = measureTime {
      var now = Clock.System.now()
      val nowNanos = (now.epochSeconds * 1_000_000_000L) + now.nanosecondsOfSecond

      val deltaNanos = (nowNanos - time).coerceAtLeast(0L)

      eventTimer.update(deltaNanos, TimeUnit.NANOSECONDS)

      count++
      counter.inc()

      val provide = ingredients.toMutableList()
      provide.remove(provide.random())

      now = Clock.System.now()
      val epochNanos = (now.epochSeconds * 1_000_000_000L) + now.nanosecondsOfSecond

      client
        .publishEvent(
          "pubsub",
          "provide",
          mutableMapOf("ingredients" to provide, "time" to epochNanos),
        )
        .subscribe()
    }
    provideTimer.update(delta.toJavaDuration())
  }
}
