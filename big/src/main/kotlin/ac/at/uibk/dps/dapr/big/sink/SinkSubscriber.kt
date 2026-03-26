package ac.at.uibk.dps.dapr.big.sink

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
@ConditionalOnProperty("app.role", havingValue = "sink")
class SinkSubscriber {

  private val sinkProxy =
    ActorProxyBuilder(SinkActor::class.java, ActorClient()).build(ActorId("sink-1"))

  @Topic(name = "register", pubsubName = "pubsub")
  @PostMapping("/register")
  fun registerSubscriber() {
    sinkProxy.register()
  }

  @Topic(name = "report", pubsubName = "pubsub")
  @PostMapping("/report")
  fun reportSubscriber(@RequestBody event: CloudEvent<Map<String, Any>>) {
    sinkProxy.report(event.data)
  }
}
