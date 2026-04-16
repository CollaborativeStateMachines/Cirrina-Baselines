package ac.at.uibk.dps.dapr.philosophers.philosopher

import io.dapr.actors.ActorType

@ActorType(name = "PhilosopherActor")
interface PhilosopherActor {
  fun hungry()

  fun onGiveRightFork()

  fun onGiveLeftFork()

  fun onRequestLeftFork()

  fun onRequestRightFork()
}
