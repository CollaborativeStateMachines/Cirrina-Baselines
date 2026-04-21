package ac.at.uibk.dps.dapr.philosophers.philosopher

import io.dapr.actors.ActorType

@ActorType(name = "PhilosopherActor")
interface PhilosopherActor {
  fun hungry()

  fun onGiveRightFork()

  fun onGiveLeftFork()

  fun onRequestLeftFork()

  fun onRequestRightFork()

  fun onJoin(data: Map<String, Any>)

  fun onInstantiate(data: Map<String, Any>)

  fun ate()

  fun thought()
}
