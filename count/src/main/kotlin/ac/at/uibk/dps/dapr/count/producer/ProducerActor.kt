package ac.at.uibk.dps.dapr.count.producer

import io.dapr.actors.ActorType

@ActorType(name = "ProducerActor")
interface ProducerActor {
  fun produce() {}
}
