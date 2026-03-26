package ac.at.uibk.dps.dapr.philosophers.arbitrator

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
@ConditionalOnProperty("app.role", havingValue = "arbitrator")
class ArbitratorPubSub {
  val arbitratorProxy: ArbitratorActor =
    ActorProxyBuilder(ArbitratorActor::class.java, ActorClient()).build(ActorId("arbitrator"))

  @Topic(name = "hungry", pubsubName = "pubsub")
  @PostMapping("/hungry")
  fun hungrySubscriber(@RequestBody(required = true) event: CloudEvent<Map<String, Any>>) {
    arbitratorProxy.hungry(event.data)
  }

  @Topic(name = "release", pubsubName = "pubsub")
  @PostMapping("/release")
  fun releaseSubscriber(@RequestBody(required = true) event: CloudEvent<Map<String, Any>>) {
    arbitratorProxy.release(event.data)
  }
}
