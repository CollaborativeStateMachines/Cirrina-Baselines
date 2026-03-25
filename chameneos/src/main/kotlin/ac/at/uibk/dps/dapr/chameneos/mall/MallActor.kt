package ac.at.uibk.dps.dapr.chameneos.mall

@io.dapr.actors.ActorType(name = "MallActor")
interface MallActor {
  fun requesting(data: Map<String, Any>)
}
