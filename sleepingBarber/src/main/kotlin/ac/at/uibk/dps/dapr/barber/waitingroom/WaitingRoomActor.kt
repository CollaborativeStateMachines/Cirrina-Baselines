package ac.at.uibk.dps.dapr.barber.waitingroom

import io.dapr.actors.ActorMethod
import io.dapr.actors.ActorType
import reactor.core.publisher.Mono

@ActorType(name = "WaitingRoom")
interface WaitingRoomActor {

  @ActorMethod(name = "newCustomerArrives") fun newCustomerArrives(customerId: Int): Mono<Void>

  @ActorMethod(name = "barberFinished") fun barberFinished(): Mono<Void>
}
