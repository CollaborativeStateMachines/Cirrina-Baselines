package ac.at.uibk.dps.dapr.barber.customer

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
@ConditionalOnProperty("app.role", havingValue = "customer")
class CustomerPubSub {
  val id = System.getenv("CUSTOMER_ID")?.toInt() ?: 0

  val customerProxy: CustomerActor? =
    ActorProxyBuilder(CustomerActor::class.java, ActorClient()).build(ActorId(id.toString()))

  @Topic(name = "full", pubsubName = "pubsub")
  @PostMapping("/full")
  fun fullSubscriber(@RequestBody event: CloudEvent<Map<String, Any>>) {
    if (event.data["id"] == id) {
      customerProxy!!.full(event.data)
    }
  }

  @Topic(name = "comeIn", pubsubName = "pubsub")
  @PostMapping("/comeIn")
  fun comeInSubscriber(@RequestBody event: CloudEvent<Map<String, Any>>) {
    if (event.data["id"] == id) {
      customerProxy!!.comeIn(event.data)
    }
  }

  @Topic(name = "done", pubsubName = "pubsub")
  @PostMapping("/done")
  fun doneSubscriber(@RequestBody event: CloudEvent<Map<String, Any>>) {
    if (event.data["id"] == id) {
      customerProxy!!.done(event.data)
    }
  }
}
