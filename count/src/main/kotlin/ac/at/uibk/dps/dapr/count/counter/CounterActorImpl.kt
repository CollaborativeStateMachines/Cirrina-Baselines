package ac.at.uibk.dps.dapr.count.counter

import io.dapr.actors.ActorId
import io.dapr.actors.runtime.AbstractActor
import io.dapr.actors.runtime.ActorRuntimeContext
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.Metrics

class CounterActorImpl(runtimeContext: ActorRuntimeContext<CounterActorImpl>, actorId: ActorId) :
  AbstractActor(runtimeContext, actorId), CounterActor {

  var count = 0
  var counter: Counter = Metrics.counter("counter.count")

  override fun increment(time: Long) {
    count += 1
    counter.increment()
  }
}
