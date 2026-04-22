package ac.at.uibk.dps.dapr.pingPong.ping

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
@ConditionalOnProperty("app.role", havingValue = "ping")
class PingSubscriber {
  private val pingProxy =
    ActorProxyBuilder(PingActor::class.java, ActorClient()).build(ActorId("ping-1"))

  @Topic(name = "pong", pubsubName = "pubsub")
  @PostMapping("/pong")
  fun pongSubscriber(@RequestBody event: CloudEvent<Long>) {
    pingProxy.ping(event.data)
  }
}
