package ac.at.uibk.dps.dapr.barber.waitingroom

import io.dapr.actors.ActorMethod
import io.dapr.actors.ActorType

@ActorType(name = "WaitingRoomActor")
interface WaitingRoomActor {
  @ActorMethod(name = "enter") fun enter(data: Map<String, Any>)

  @ActorMethod(name = "ready") fun ready(data: Map<String, Any>)
}
