package ac.at.uibk.dps.dapr.philosophers.philosopher

import ac.at.uibk.dps.dapr.philosophers.DiningPhilosophers
import ac.at.uibk.dps.dapr.philosophers.arbitrator.ArbitratorPubSub
import io.dapr.actors.ActorId
import io.dapr.actors.runtime.AbstractActor
import io.dapr.actors.runtime.ActorRuntimeContext
import java.time.Duration
import java.util.concurrent.TimeUnit
import kotlin.time.measureTime
import kotlin.time.toJavaDuration
import reactor.core.publisher.Mono

class PhilosopherActorImpl(runtimeContext: ActorRuntimeContext<PhilosopherActorImpl>, id: ActorId) :
  AbstractActor(runtimeContext, id), PhilosopherActor {

  companion object {
    const val COUNTER_NAME = "philosopher.meals"
    const val EVENT_TIMER_NAME = "event.latency"
    const val EAT_DURATION_NAME = "eat.duration"
  }

  private val eatingDuration = System.getenv("EATING_DURATION")?.toInt() ?: 0

  var completedRounds: Int = 0

  val metricsRegistry = DiningPhilosophers.provideMetricRegistry()

  override fun start(): Mono<Void> {
    return ArbitratorPubSub.requestForks(DiningPhilosophers.daprClient, getMap())
  }

  override fun eat(data: Map<String, Any>): Mono<Void> {
    val time = data["time"] as Long
    metricsRegistry.timer(EVENT_TIMER_NAME)!!.update((System.nanoTime() - time) / 1_000, TimeUnit.MICROSECONDS)
    val delta = measureTime {
      completedRounds++
      metricsRegistry.counter(COUNTER_NAME).inc(1L)
      val delay =
        Mono.delay(Duration.ofMillis(eatingDuration.toLong())).flatMap {
          ArbitratorPubSub.doneEating(DiningPhilosophers.daprClient, getMap())
        }
      delay.then(ArbitratorPubSub.requestForks(DiningPhilosophers.daprClient, getMap())).subscribe()
    }
    metricsRegistry.timer(EAT_DURATION_NAME).update(delta.toJavaDuration())
    return Mono.empty()
  }

  private fun getMap(): Map<String, Any> = mapOf("id" to id.toString(), "time" to System.nanoTime())
}
