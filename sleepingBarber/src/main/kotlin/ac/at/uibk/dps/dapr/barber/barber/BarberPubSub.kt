package ac.at.uibk.dps.dapr.barber.barber

import ac.at.uibk.dps.dapr.barber.SleepingBarber
import io.dapr.Topic
import io.dapr.actors.ActorId
import io.dapr.actors.client.ActorProxyBuilder
import io.dapr.client.DaprClient
import io.dapr.client.domain.CloudEvent
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RestController
import reactor.core.publisher.Mono

@RestController
@ConditionalOnProperty("app.role", havingValue = "barber")
class BarberPubSub {

  companion object {
    const val PUB_SUB_NAME = "barber_pub_sub"
    const val CUTTING_TOPIC = "cutting"
    const val BARBER_NAME = "barber"

    fun cutting(client: DaprClient, id: Int): Mono<Void> {
      return client.publishEvent(PUB_SUB_NAME, CUTTING_TOPIC, id)
    }
  }

  val barberActor: BarberActor? =
      ActorProxyBuilder(BarberActor::class.java, SleepingBarber.actorClient)
          .build(ActorId(BARBER_NAME))

  @Topic(name = CUTTING_TOPIC, pubsubName = PUB_SUB_NAME)
  @PostMapping("/$CUTTING_TOPIC")
  fun cuttingSubscriber(@RequestBody event: CloudEvent<Int>) {
    barberActor!!.cuttingHair(event.data).subscribe()
  }
}
