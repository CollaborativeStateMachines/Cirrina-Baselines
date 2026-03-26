package ac.at.uibk.dps.dapr.cigarette.arbiter

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
@ConditionalOnProperty("app.role", havingValue = "arbiter")
class ArbiterSubscriber {

  private val arbiterProxy =
    ActorProxyBuilder(ArbiterActor::class.java, ActorClient()).build(ActorId("arbiter-1"))

  @Topic(name = "finish", pubsubName = "pubsub")
  @PostMapping("/finish")
  fun finishSubscriber(@RequestBody event: CloudEvent<Long>) {
    arbiterProxy.provide(event.data)
  }
}
