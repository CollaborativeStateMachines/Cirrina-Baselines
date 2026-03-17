package ac.at.uibk.dps.dapr.philosophers.philosopher

import io.dapr.Topic
import io.dapr.actors.ActorId
import io.dapr.actors.client.ActorClient
import io.dapr.actors.client.ActorProxyBuilder
import io.dapr.client.DaprClient
import io.dapr.client.domain.CloudEvent
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RestController
import reactor.core.publisher.Mono

@RestController
@ConditionalOnProperty("app.role", havingValue = "philosopher")
class PhilosopherPubSub {

  companion object {
    const val EAT_TOPIC_NAME = "eat"
    const val PUB_SUB_NAME = "philosopher_pub_sub"

    fun eat(client: DaprClient, id: Int): Mono<Void> {
      return client.publishEvent(PUB_SUB_NAME, EAT_TOPIC_NAME, id)
    }
  }

  private val id = System.getenv("PHILOSOPHER_ID")

  private val philosopherProxy =
    ActorProxyBuilder(PhilosopherActor::class.java, ActorClient()).build(ActorId("$id"))

  @Topic(name = EAT_TOPIC_NAME, pubsubName = PUB_SUB_NAME)
  @PostMapping("/eat")
  fun eatSubscriber(@RequestBody event: CloudEvent<Int>) {
    if (event.data == id.toInt()) {
      philosopherProxy.eat().subscribe()
    }
  }
}
