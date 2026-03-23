package ac.at.uibk.dps.dapr.barber.waitingroom

import ac.at.uibk.dps.dapr.barber.SleepingBarber
import ac.at.uibk.dps.dapr.barber.barber.BarberPubSub
import ac.at.uibk.dps.dapr.barber.customer.CustomerPubSub
import io.dapr.actors.ActorId
import io.dapr.actors.runtime.AbstractActor
import io.dapr.actors.runtime.ActorRuntimeContext
import java.util.LinkedList
import java.util.Queue
import reactor.core.publisher.Mono

class WaitingRoomActorImpl(runtimeContext: ActorRuntimeContext<WaitingRoomActorImpl>, id: ActorId) :
    AbstractActor(runtimeContext, id), WaitingRoomActor {

  val client = SleepingBarber.daprClient

  val roomSlots = System.getenv("ROOM_SLOTS")?.toInt() ?: 3

  var barberBusy = false

  val queue: Queue<Int> = LinkedList()

  override fun newCustomerArrives(customerId: Int): Mono<Void> {
    if (roomSlots <= queue.size) {
      return CustomerPubSub.full(client, customerId)
    } else if (!queue.isEmpty() || barberBusy) {
      queue.add(customerId)
    } else {
      barberBusy = true
      return BarberPubSub.cutting(client, customerId)
    }
    return Mono.empty()
  }

  override fun barberFinished(): Mono<Void> {
    if (queue.isEmpty()) {
      barberBusy = false
      return Mono.empty()
    } else {
      val nextId = queue.poll()
      return BarberPubSub.cutting(client, nextId)
    }
  }
}
