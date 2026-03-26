package ac.at.uibk.dps.dapr.philosophers.philosopher

import io.dapr.actors.ActorMethod
import io.dapr.actors.ActorType

@ActorType(name = "PhilosopherActor")
interface PhilosopherActor {
  @ActorMethod(name = "starting") fun starting()

  @ActorMethod(name = "eating") fun eating(data: Map<String, Any>)
}
