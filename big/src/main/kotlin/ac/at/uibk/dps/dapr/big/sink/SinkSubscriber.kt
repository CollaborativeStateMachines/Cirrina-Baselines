package ac.at.uibk.dps.dapr.big.sink

import io.dapr.Topic
import io.dapr.actors.ActorId
import io.dapr.actors.client.ActorClient
import io.dapr.actors.client.ActorProxyBuilder
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
  fun handleRegister(@RequestBody body: Map<String, Any>) {
    val actor = body["data"] as String
    sinkProxy.register(actor)
  }

  @Topic(name = "done", pubsubName = "pubsub")
  @PostMapping("/done")
  fun handleDone(@RequestBody body: Map<String, Any>) {
    val actor = body["data"] as String

    sinkProxy.receiveDone(actor)
  }
}
