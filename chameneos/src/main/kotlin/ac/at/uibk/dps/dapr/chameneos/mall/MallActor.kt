package ac.at.uibk.dps.dapr.chameneos.mall

data class MallRequest(val requester: String = "", val color: Int = 0)

@io.dapr.actors.ActorType(name = "MallActor")
interface MallActor {
  fun requesting(request: MallRequest)
}
