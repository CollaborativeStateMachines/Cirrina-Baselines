package ac.at.uibk.dps.dapr.barber.barber

import io.dapr.actors.ActorMethod
import io.dapr.actors.ActorType

@ActorType(name = "BarberActor")
interface BarberActor {
  @ActorMethod(name = "sleeping") fun sleeping()

  @ActorMethod(name = "cutting") fun cutting(data: Map<String, Any>)
}
