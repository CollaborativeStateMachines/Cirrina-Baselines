package ac.at.uibk.dps.dapr.count.counter

import io.dapr.actors.ActorType

@ActorType(name = "CounterActor")
interface CounterActor {
  fun increment(time: Long)
}
