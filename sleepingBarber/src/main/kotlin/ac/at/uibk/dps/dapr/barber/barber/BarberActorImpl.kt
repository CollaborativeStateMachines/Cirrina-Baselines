package ac.at.uibk.dps.dapr.barber.barber

import ac.at.uibk.dps.dapr.barber.SleepingBarber
import ac.at.uibk.dps.dapr.barber.customer.CustomerPubSub
import ac.at.uibk.dps.dapr.barber.waitingroom.WaitingRoomPubSub
import io.dapr.actors.ActorId
import io.dapr.actors.runtime.AbstractActor
import io.dapr.actors.runtime.ActorRuntimeContext
import java.time.Duration
import reactor.core.publisher.Mono

class BarberActorImpl(runtimeContext: ActorRuntimeContext<BarberActorImpl>, id: ActorId) :
  AbstractActor(runtimeContext, id), BarberActor {

  val cuttingTimeMS = System.getenv("CUTTING_TIME_MS")?.toInt() ?: 0

  override fun cuttingHair(customerId: Int): Mono<Void> {
    return Mono.delay(Duration.ofMillis(cuttingTimeMS.toLong())).flatMap {
      Mono.`when`(
        WaitingRoomPubSub.barberFinished(SleepingBarber.daprClient),
        CustomerPubSub.done(SleepingBarber.daprClient, customerId),
      )
    }
  }
}
