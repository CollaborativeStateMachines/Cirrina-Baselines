package ac.at.uibk.dps.dapr.philosophers.philosopher

import io.dapr.actors.ActorType

@ActorType(name = "PhilosopherActor")
interface PhilosopherActor {
  fun hungry()

  fun onGiveRightFork(data: Map<String, Any>)

  fun onGiveLeftFork(data: Map<String, Any>)

  fun onRequestLeftFork(data: Map<String, Any>)

  fun onRequestRightFork(data: Map<String, Any>)

  fun onJoin(data: Map<String, Any>)

  fun onInstantiate(data: Map<String, Any>)

  fun ate()

  fun thought()
}
