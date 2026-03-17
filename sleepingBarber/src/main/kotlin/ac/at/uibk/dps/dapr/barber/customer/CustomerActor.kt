package ac.at.uibk.dps.dapr.barber.customer

import io.dapr.actors.ActorMethod
import io.dapr.actors.ActorType
import reactor.core.publisher.Mono

@ActorType(name = "CustomerActor")
interface CustomerActor {

  @ActorMethod(name = "enterWaitingRoom") fun enterWaitingRoom(): Mono<Void>

  @ActorMethod(name = "waitingRoomFull") fun waitingRoomFull(): Mono<Void>

  @ActorMethod(name = "doneCutting") fun doneCutting(): Mono<Void>
}
