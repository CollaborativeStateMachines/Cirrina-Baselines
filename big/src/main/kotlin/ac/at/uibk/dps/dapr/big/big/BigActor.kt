package ac.at.uibk.dps.dapr.big.big

import io.dapr.actors.ActorType

@ActorType(name = "BigActor")
interface BigActor {
  fun register()

  fun initial()

  fun onPong(data: Map<String, Any>)

  fun onPing(data: Map<String, Any>)
}
