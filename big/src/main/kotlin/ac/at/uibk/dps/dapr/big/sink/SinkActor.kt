package ac.at.uibk.dps.dapr.big.sink

import io.dapr.actors.ActorType

@ActorType(name = "SinkActor")
interface SinkActor {
  fun register(actor: String)

  fun sendNeighbors()

  fun receiveDone(sender: String)
}
