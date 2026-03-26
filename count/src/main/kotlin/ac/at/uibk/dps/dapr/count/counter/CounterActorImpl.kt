package ac.at.uibk.dps.dapr.count.counter

import ac.at.uibk.dps.dapr.count.Count
import io.dapr.actors.ActorId
import io.dapr.actors.runtime.AbstractActor
import io.dapr.actors.runtime.ActorRuntimeContext
import java.util.concurrent.TimeUnit
import kotlin.time.Clock

class CounterActorImpl(runtimeContext: ActorRuntimeContext<CounterActorImpl>, actorId: ActorId) :
  AbstractActor(runtimeContext, actorId), CounterActor {

  var count = 0

  var metricRegistry = Count.provideMetricRegistry()

  override fun increment(time: Long) {
    val now = Clock.System.now()
    val nowNanos = (now.epochSeconds * 1_000_000_000L) + now.nanosecondsOfSecond
    val deltaNanos = (nowNanos - time).coerceAtLeast(0L)

    metricRegistry.timer("event.latency").update((deltaNanos), TimeUnit.NANOSECONDS)

    ++count
    metricRegistry.counter("counter.count").inc()
  }
}
