package ac.at.uibk.dps.dapr.barber.barber

import io.dapr.Topic
import io.dapr.actors.ActorId
import io.dapr.actors.client.ActorClient
import io.dapr.actors.client.ActorProxyBuilder
import io.dapr.client.domain.CloudEvent
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RestController

@RestController
@ConditionalOnProperty("app.role", havingValue = "barber")
class BarberPubSub {
  val barberActor =
    ActorProxyBuilder(BarberActor::class.java, ActorClient()).build(ActorId("barber"))

  @Topic(name = "sit", pubsubName = "pubsub")
  @PostMapping("/sit")
  fun sittingSubscriber(@RequestBody event: CloudEvent<Map<String, Any>>) {
    barberActor.cutting(event.data)
  }
}
