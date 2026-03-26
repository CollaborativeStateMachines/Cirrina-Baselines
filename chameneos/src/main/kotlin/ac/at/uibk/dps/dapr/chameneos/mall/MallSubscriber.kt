package ac.at.uibk.dps.dapr.chameneos.mall

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
@ConditionalOnProperty("app.role", havingValue = "mall")
class MallSubscriber {
  private val mallProxy =
    ActorProxyBuilder(MallActor::class.java, ActorClient()).build(ActorId("mall-1"))

  @Topic(name = "requesting", pubsubName = "pubsub")
  @PostMapping("/requesting")
  fun requestSubscriber(@RequestBody event: CloudEvent<Map<String, Any>>) {
    mallProxy.requesting(event.data)
  }
}
