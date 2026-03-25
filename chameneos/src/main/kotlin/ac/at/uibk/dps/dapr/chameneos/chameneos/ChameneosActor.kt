package ac.at.uibk.dps.dapr.chameneos.chameneos

import io.dapr.actors.ActorType

@ActorType(name = "ChameneosActor")
interface ChameneosActor {
  fun request()

  fun meet(data: Map<String, Any>)

  fun change(data: Map<String, Any>)
}
