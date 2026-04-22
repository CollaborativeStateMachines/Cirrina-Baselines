package ac.at.uibk.dps.dapr.pingPong.pong

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
@ConditionalOnProperty("app.role", havingValue = "pong")
class PongSubscriber {
  private val pongProxy =
    ActorProxyBuilder(PongActor::class.java, ActorClient()).build(ActorId("pong-1"))

  @Topic(name = "ping", pubsubName = "pubsub")
  @PostMapping("/ping")
  fun pingSubscriber(@RequestBody event: CloudEvent<Long>) {
    pongProxy.pong(event.data)
  }
}
