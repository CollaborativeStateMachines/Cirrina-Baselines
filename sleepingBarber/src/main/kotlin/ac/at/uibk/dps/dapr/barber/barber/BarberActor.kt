package ac.at.uibk.dps.dapr.barber.barber

import io.dapr.actors.ActorMethod
import io.dapr.actors.ActorType
import reactor.core.publisher.Mono

@ActorType(name = "BarberActor")
interface BarberActor {

  @ActorMethod(name = "cuttingHair") fun cuttingHair(customerId: Int): Mono<Void>
}
