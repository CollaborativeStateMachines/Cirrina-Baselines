package ac.at.uibk.dps.dapr.big.big

import io.dapr.actors.ActorType

@ActorType(name = "BigActor")
interface BigActor {
  fun register()

  fun assignNeighbors(neighbors: List<String>)

  fun receivePong(data: Map<String, Any>)

  fun sendPong(data: Map<String, Any>)
}
