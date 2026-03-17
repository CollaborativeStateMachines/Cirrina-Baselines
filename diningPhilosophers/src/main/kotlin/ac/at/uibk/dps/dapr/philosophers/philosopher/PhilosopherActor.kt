package ac.at.uibk.dps.dapr.philosophers.philosopher

import io.dapr.actors.ActorMethod
import io.dapr.actors.ActorType
import reactor.core.publisher.Mono

@ActorType(name = "PhilosopherActor")
interface PhilosopherActor {

  @ActorMethod(name = "eat") fun eat(): Mono<Void>

  @ActorMethod(name = "start") fun start(): Mono<Void>
}
