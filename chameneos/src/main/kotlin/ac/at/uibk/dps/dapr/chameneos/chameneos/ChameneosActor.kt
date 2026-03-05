package ac.at.uibk.dps.dapr.chameneos.chameneos

import io.dapr.actors.ActorType

data class MeetRequest(val partner: String = "", val partnerColor: Int = 0)

@ActorType(name = "ChameneosActor")
interface ChameneosActor {
  fun request()

  fun meet(request: MeetRequest)

  fun change(partnerColor: Int)
}
