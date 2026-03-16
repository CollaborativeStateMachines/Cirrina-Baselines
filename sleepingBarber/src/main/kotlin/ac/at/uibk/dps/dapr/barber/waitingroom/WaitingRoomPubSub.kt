package ac.at.uibk.dps.dapr.barber.waitingroom

import io.dapr.Topic
import io.dapr.actors.ActorId
import io.dapr.actors.client.ActorClient
import io.dapr.actors.client.ActorProxyBuilder
import io.dapr.client.DaprClient
import io.dapr.client.domain.CloudEvent
import kotlin.jvm.java
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RestController
import reactor.core.publisher.Mono

@RestController
@ConditionalOnProperty("app.role", havingValue = "waiting_room")
class WaitingRoomPubSub {
  companion object {
    private const val PUB_SUB_NAME = "waiting_room_pub_sub"
    private const val NEW_CUSTOMER_TOPIC = "new_customer_topic"
    private const val BARBER_FINISHED_TOPIC = "barber_finished_topic"
    const val WAITING_ROOM_NAME = "waitingRoom"

    fun newCustomer(client: DaprClient, id: Int): Mono<Void> {
      return client.publishEvent(PUB_SUB_NAME, NEW_CUSTOMER_TOPIC, id)
    }

    fun barberFinished(client: DaprClient): Mono<Void> {
      return client.publishEvent(PUB_SUB_NAME, BARBER_FINISHED_TOPIC, Unit)
    }
  }

  val waitingRoomActor: WaitingRoomActor? =
    ActorProxyBuilder(WaitingRoomActor::class.java, ActorClient()).build(ActorId(WAITING_ROOM_NAME))

  @Topic(name = NEW_CUSTOMER_TOPIC, pubsubName = PUB_SUB_NAME)
  @PostMapping("/$NEW_CUSTOMER_TOPIC")
  fun newCustomerSubscriber(@RequestBody(required = true) event: CloudEvent<Int>) {
    waitingRoomActor!!.newCustomerArrives(event.data).subscribe()
  }

  @Topic(name = BARBER_FINISHED_TOPIC, pubsubName = PUB_SUB_NAME)
  @PostMapping("/$BARBER_FINISHED_TOPIC")
  fun barberFinishedSubscriber() {
    waitingRoomActor!!.barberFinished().subscribe()
  }
}
