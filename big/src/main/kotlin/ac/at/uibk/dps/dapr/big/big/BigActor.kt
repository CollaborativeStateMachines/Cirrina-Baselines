package ac.at.uibk.dps.dapr.big.big

import io.dapr.actors.ActorType

@ActorType(name = "BigActor")
interface BigActor {
  fun register()

  fun assignNeighbors(neighbors: List<String>)

  fun receivePong(sender: String)

  fun sendPing()

  fun sendPong(sender: String)
}
