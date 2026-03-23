package ac.at.uibk.dps.dapr.barber.customer

import ac.at.uibk.dps.dapr.barber.SleepingBarber
import ac.at.uibk.dps.dapr.barber.waitingroom.WaitingRoomPubSub
import io.dapr.actors.ActorId
import io.dapr.actors.runtime.AbstractActor
import io.dapr.actors.runtime.ActorRuntimeContext
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.Metrics
import reactor.core.publisher.Mono

class CustomerActorImpl(runtimeContext: ActorRuntimeContext<CustomerActorImpl>, id: ActorId) :
  AbstractActor(runtimeContext, id), CustomerActor {

  companion object {
    const val COUNTER_NAME = "customer_rounds"
  }

  var completedRounds = 0

  var metricsCounter: Counter? = Metrics.counter(COUNTER_NAME, "customer", id.toString())

  override fun enterWaitingRoom(): Mono<Void> {
    return WaitingRoomPubSub.newCustomer(SleepingBarber.daprClient, id.toString().toInt())
  }

  override fun waitingRoomFull(): Mono<Void> {
    return WaitingRoomPubSub.newCustomer(SleepingBarber.daprClient, id.toString().toInt())
  }

  override fun doneCutting(): Mono<Void> {
    completedRounds++
    metricsCounter!!.increment()
    return enterWaitingRoom()
  }
}
