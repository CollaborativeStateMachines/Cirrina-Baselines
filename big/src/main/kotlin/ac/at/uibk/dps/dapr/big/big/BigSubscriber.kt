package ac.at.uibk.dps.dapr.big.big

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
@ConditionalOnProperty("app.role", havingValue = "big")
class BigSubscriber {
  private val id = System.getenv("BIG_ID")

  private val bigProxy =
    ActorProxyBuilder(BigActor::class.java, ActorClient()).build(ActorId("$id"))

  @Topic(name = "initial", pubsubName = "pubsub")
  @PostMapping("/initial")
  fun initialSubscriber() {
    bigProxy.initial()
  }

  @Topic(name = "ping", pubsubName = "pubsub")
  @PostMapping("/ping")
  fun pingSubscriber(@RequestBody event: CloudEvent<Map<String, Any>>) {
    if (event.data["target"].toString() == id) {
      bigProxy.onPing(event.data)
    }
  }

  @Topic(name = "pong", pubsubName = "pubsub")
  @PostMapping("/pong")
  fun pongSubscriber(@RequestBody event: CloudEvent<Map<String, Any>>) {
    if (event.data["target"] == id) {
      bigProxy.onPong(event.data)
    }
  }
}
