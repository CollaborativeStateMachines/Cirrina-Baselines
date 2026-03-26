package ac.at.uibk.dps.dapr.philosophers.arbitrator

import io.dapr.actors.ActorMethod
import io.dapr.actors.ActorType

@ActorType(name = "ArbitratorActor")
interface ArbitratorActor {
  @ActorMethod(name = "hungry") fun hungry(data: Map<String, Any>)

  @ActorMethod(name = "release") fun release(data: Map<String, Any>)
}
