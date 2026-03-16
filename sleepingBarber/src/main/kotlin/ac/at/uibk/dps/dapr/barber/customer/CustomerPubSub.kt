package ac.at.uibk.dps.dapr.barber.customer

import io.dapr.Topic
import io.dapr.actors.ActorId
import io.dapr.actors.client.ActorClient
import io.dapr.actors.client.ActorProxyBuilder
import io.dapr.client.DaprClient
import io.dapr.client.domain.CloudEvent
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RestController
import reactor.core.publisher.Mono

@RestController
@ConditionalOnProperty("app.role", havingValue = "customer")
class CustomerPubSub {

  companion object {
    private const val PUB_SUB_NAME = "customer_pub_sub"
    private const val ENTER_TOPIC = "enter"
    private const val FULL_TOPIC = "full"
    private const val DONE_TOPIC = "done"

    fun enter(client: DaprClient, id: Int): Mono<Void> {
      return client.publishEvent(PUB_SUB_NAME, ENTER_TOPIC, id)
    }

    fun full(client: DaprClient, id: Int): Mono<Void> {
      return client.publishEvent(PUB_SUB_NAME, FULL_TOPIC, id)
    }

    fun done(client: DaprClient, id: Int): Mono<Void> {
      return client.publishEvent(PUB_SUB_NAME, DONE_TOPIC, id)
    }
  }

  val id = System.getenv("CUSTOMER_ID")?.toInt() ?: 0

  val customerProxy: CustomerActor? =
    ActorProxyBuilder(CustomerActor::class.java, ActorClient()).build(ActorId(id.toString()))

  @Topic(name = ENTER_TOPIC, pubsubName = PUB_SUB_NAME)
  @PostMapping("/$ENTER_TOPIC")
  fun enterSubscriber(@RequestBody event: CloudEvent<Int>) {
    if (event.data == id) {
      customerProxy!!.enterWaitingRoom().subscribe()
    }
  }

  @Topic(name = FULL_TOPIC, pubsubName = PUB_SUB_NAME)
  @PostMapping("/$FULL_TOPIC")
  fun fullSubscriber(@RequestBody event: CloudEvent<Int>) {
    if (event.data == id) {
      customerProxy!!.waitingRoomFull().subscribe()
    }
  }

  @Topic(name = DONE_TOPIC, pubsubName = PUB_SUB_NAME)
  @PostMapping("/$DONE_TOPIC")
  fun doneSubscriber(@RequestBody event: CloudEvent<Int>) {
    if (event.data == id) {
      customerProxy!!.doneCutting().subscribe()
    }
  }
}
