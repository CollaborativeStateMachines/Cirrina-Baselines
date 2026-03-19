package ac.at.uibk.dps.dapr.philosophers.arbitrator

import ac.at.uibk.dps.dapr.philosophers.DiningPhilosophers
import ac.at.uibk.dps.dapr.philosophers.philosopher.PhilosopherPubSub
import io.dapr.actors.ActorId
import io.dapr.actors.runtime.AbstractActor
import io.dapr.actors.runtime.ActorRuntimeContext
import java.util.concurrent.TimeUnit
import kotlin.time.measureTime
import kotlin.time.toJavaDuration
import reactor.core.publisher.Mono

class ArbitratorActorImpl(
  runtimeContext: ActorRuntimeContext<ArbitratorActorImpl>,
  id: ActorId,
  val numberOfPhilosophers: Int,
) : AbstractActor(runtimeContext, id), ArbitratorActor {

  companion object {
    const val EVENT_TIMER_NAME = "event.latency"
    const val REQUEST_DURATION = "request.duration"
    const val DONE_DURATION = "done.duration"
  }

  private val forks = BooleanArray(numberOfPhilosophers) { true }

  private val waiting = BooleanArray(numberOfPhilosophers) { false }

  val metricsRegistry = DiningPhilosophers.provideMetricRegistry()

  private fun tryAssign(pos: Int) {
    if (forks[pos] && forks[next(pos)]) {
      forks[pos] = false
      forks[next(pos)] = false
      waiting[pos] = false
      PhilosopherPubSub.eat(DiningPhilosophers.daprClient, getMap(pos)).subscribe()
    } else {
      waiting[pos] = true
    }
  }

  override fun requestForks(data: Map<String, Any>): Mono<Void> {
    measureEventTime(data)
    val duration = measureTime {
      val pos = data["id"].toString().toInt()
      if (pos in 0 until numberOfPhilosophers) tryAssign(pos)
      else DiningPhilosophers.logger.info("Invalid philosopher position: $pos")
    }
    metricsRegistry.timer(REQUEST_DURATION).update(duration.toJavaDuration())
    return Mono.empty()
  }

  override fun doneEating(data: Map<String, Any>): Mono<Void> {
    measureEventTime(data)
    val duration = measureTime {
      val pos = data["id"].toString().toInt()
      forks[pos] = true
      forks[next(pos)] = true
      listOf(next(pos), prev(pos), pos).filter { waiting[it] }.forEach { tryAssign(it) }
    }
    metricsRegistry.timer(DONE_DURATION).update(duration.toJavaDuration())
    return Mono.empty()
  }

  private fun next(i: Int) = (i + 1) % numberOfPhilosophers

  private fun prev(i: Int) = (i - 1 + numberOfPhilosophers) % numberOfPhilosophers

  private fun getMap(i: Int) = mapOf("id" to i, "time" to System.currentTimeMillis())

  private fun measureEventTime(data: Map<String, Any>) =
    metricsRegistry
      .timer(EVENT_TIMER_NAME)
      .update((System.currentTimeMillis() - data["time"] as Long), TimeUnit.MILLISECONDS)
}
