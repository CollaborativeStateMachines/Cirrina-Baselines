package ac.at.uibk.dps.dapr.cigarette.smoker

import io.dapr.actors.ActorType

@ActorType(name = "SmokerActor")
interface SmokerActor {
  fun smoke(data: Map<String, Any>)
}
