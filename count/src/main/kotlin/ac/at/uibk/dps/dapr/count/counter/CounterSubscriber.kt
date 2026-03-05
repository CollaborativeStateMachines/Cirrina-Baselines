package ac.at.uibk.dps.dapr.count.counter

import io.dapr.Topic
import io.dapr.actors.ActorId
import io.dapr.actors.client.ActorClient
import io.dapr.actors.client.ActorProxyBuilder
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RestController

@RestController
@ConditionalOnProperty("app.role", havingValue = "counter")
class CounterSubscriber {
  private val actorClient = ActorClient()
  private val counterProxy =
    ActorProxyBuilder(CounterActor::class.java, actorClient).build(ActorId("counter-1"))

  @Topic(name = "increment", pubsubName = "pubsub")
  @PostMapping("/increment")
  fun handleIncrement(@RequestBody body: Map<String, Any>) {
    counterProxy.increment(body["data"] as Long)
  }
}
