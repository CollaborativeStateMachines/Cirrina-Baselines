package ac.at.uibk.dps.dapr.philosophers.philosopher

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
@ConditionalOnProperty("app.role", havingValue = "philosopher")
class PhilosopherPubSub {
  private val id = System.getenv("PHILOSOPHER_ID")

  private val philosopherProxy =
    ActorProxyBuilder(PhilosopherActor::class.java, ActorClient()).build(ActorId("$id"))

  @Topic(name = "acquire", pubsubName = "pubsub")
  @PostMapping("/acquire")
  fun acquireSubscriber(@RequestBody event: CloudEvent<Map<String, Any>>) {
    if (event.data["id"] as Int == id.toInt()) {
      philosopherProxy.eating(event.data)
    }
  }
}
