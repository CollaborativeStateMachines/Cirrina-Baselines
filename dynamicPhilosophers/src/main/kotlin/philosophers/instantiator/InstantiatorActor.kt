package ac.at.uibk.dps.dapr.philosophers.instantiator

import io.dapr.actors.ActorType

@ActorType(name = "InstantiatorActor")
interface InstantiatorActor {
  fun onJoin(data: Map<String, Any>)

  fun waitTurn()

  fun instantiate()
}
