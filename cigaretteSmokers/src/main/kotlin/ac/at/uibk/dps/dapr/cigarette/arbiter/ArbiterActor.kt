package ac.at.uibk.dps.dapr.cigarette.arbiter

import io.dapr.actors.ActorType

@ActorType(name = "ArbiterActor")
interface ArbiterActor {
  fun provide()
}
