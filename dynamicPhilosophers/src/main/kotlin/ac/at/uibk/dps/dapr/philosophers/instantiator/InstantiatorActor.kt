package ac.at.uibk.dps.dapr.philosophers.instantiator

import io.dapr.actors.ActorType

@ActorType(name = "InstantiatorActor")
interface InstantiatorActor {
  fun waitTurn()

  fun instantiate()

  fun onNodeCreated(data: Map<String, Any>)
}
