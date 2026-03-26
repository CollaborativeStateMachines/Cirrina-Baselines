package ac.at.uibk.dps.dapr.big.sink

import io.dapr.actors.ActorType

@ActorType(name = "SinkActor")
interface SinkActor {
  fun report(data: Map<String, Any>)

  fun register()
}
