package ac.at.uibk.dps.dapr.pingPong.ping

import io.dapr.actors.ActorType

@ActorType(name = "PingActor")
interface PingActor {
  fun ping(time: Long)
}
