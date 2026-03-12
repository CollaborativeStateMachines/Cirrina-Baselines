package ac.at.uibk.dps.dapr.philosophers.philosopher

import ac.at.uibk.dps.dapr.philosophers.DiningPhilosophers
import ac.at.uibk.dps.dapr.philosophers.arbitrator.ArbitratorPubSub
import io.dapr.actors.ActorId
import io.dapr.actors.runtime.AbstractActor
import io.dapr.actors.runtime.ActorRuntimeContext
import io.micrometer.core.instrument.Metrics
import java.time.Duration
import reactor.core.publisher.Mono

class PhilosopherActorImpl(runtimeContext: ActorRuntimeContext<PhilosopherActorImpl>, id: ActorId) :
  AbstractActor(runtimeContext, id), PhilosopherActor {

  companion object {
    const val COUNTER_NAME = "total_meals"
  }

  private val eatingDuration = System.getenv("EATING_DURATION")?.toInt() ?: 0

  var completedRounds: Int = 0

  var metricsCounter = Metrics.counter(COUNTER_NAME, "philosopher", id.toString())

  override fun start(): Mono<Void> {
    return ArbitratorPubSub.requestForks(DiningPhilosophers.daprClient, id.toString().toInt())
  }

  override fun eat(): Mono<Void> {
    completedRounds++
    metricsCounter.increment()
    val delay =
      Mono.delay(Duration.ofMillis(eatingDuration.toLong())).flatMap {
        ArbitratorPubSub.doneEating(DiningPhilosophers.daprClient, id.toString().toInt())
      }
    return delay.then(
      ArbitratorPubSub.requestForks(DiningPhilosophers.daprClient, id.toString().toInt())
    )
  }
}
