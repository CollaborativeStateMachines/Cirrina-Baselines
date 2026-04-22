package ac.at.uibk.dps.dapr.pingPong.pong

import io.dapr.actors.ActorType

@ActorType(name = "PongActor")
interface PongActor {
  fun pong(time: Long)
}
