package ac.at.uibk.dps.dapr.philosophers.arbitrator

import io.dapr.actors.ActorMethod
import io.dapr.actors.ActorType
import reactor.core.publisher.Mono

@ActorType(name = "ArbitratorActor")
interface ArbitratorActor {

  @ActorMethod(name = "requestForks") fun requestForks(position: Int): Mono<Void>

  @ActorMethod(name = "doneEating") fun doneEating(pos: Int): Mono<Void>
}
