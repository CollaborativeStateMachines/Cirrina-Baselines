package ac.at.uibk.dps.dapr.count.counter

import ac.at.uibk.dps.dapr.count.Count
import io.dapr.actors.ActorId
import io.dapr.actors.runtime.AbstractActor
import io.dapr.actors.runtime.ActorRuntimeContext
import java.util.concurrent.TimeUnit
import kotlin.time.Clock
import kotlin.time.measureTime
import kotlin.time.toJavaDuration

class CounterActorImpl(runtimeContext: ActorRuntimeContext<CounterActorImpl>, actorId: ActorId) :
  AbstractActor(runtimeContext, actorId), CounterActor {

  var count = 0

  var metricRegistry = Count.provideMetricRegistry()
  var counter = metricRegistry.counter("counter.count")
  var eventTimer = metricRegistry.timer("event.latency")
  var incrementTimer = metricRegistry.timer("increment.duration")

  override fun increment(time: Long) {
    val delta = measureTime {
      val now = Clock.System.now()
      val nowNanos = (now.epochSeconds * 1_000_000_000L) + now.nanosecondsOfSecond

      val deltaNanos = (nowNanos - time).coerceAtLeast(0L)

      eventTimer.update((deltaNanos), TimeUnit.NANOSECONDS)

      count += 1
      counter.inc()
    }
    incrementTimer.update(delta.toJavaDuration())
  }
}
