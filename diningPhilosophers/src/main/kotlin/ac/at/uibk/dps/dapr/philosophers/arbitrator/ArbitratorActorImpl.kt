package ac.at.uibk.dps.dapr.philosophers.arbitrator

import ac.at.uibk.dps.dapr.philosophers.DiningPhilosophers
import io.dapr.actors.ActorId
import io.dapr.actors.runtime.AbstractActor
import io.dapr.actors.runtime.ActorRuntimeContext
import io.dapr.client.DaprClient
import io.dapr.client.DaprClientBuilder
import java.util.concurrent.TimeUnit
import kotlin.time.Clock

class ArbitratorActorImpl(runtimeContext: ActorRuntimeContext<ArbitratorActorImpl>, id: ActorId) :
  AbstractActor(runtimeContext, id), ArbitratorActor {
  private val client: DaprClient = DaprClientBuilder().build()

  private val forks = BooleanArray(6) { false }

  private val waiting = BooleanArray(6) { false }

  val metricsRegistry = DiningPhilosophers.provideMetricRegistry()

  override fun hungry(data: Map<String, Any>) {
    measureEventTime(data)
    val id = data["id"].toString().toInt()

    if (!(forks[id] || forks[(id + 1) % 6])) {
      forks[id] = true
      forks[(id + 1) % 6] = true
      client.publishEvent("pubsub", "acquire", getMap(id)).subscribe()
    } else {
      waiting[id] = true
    }
  }

  override fun release(data: Map<String, Any>) {
    measureEventTime(data)
    val id = data["id"].toString().toInt()

    forks[id] = false
    forks[(id + 1) % 6] = false

    test((id + 6 - 1) % 6)
    test((id + 1) % 6)
  }

  private fun test(nid: Int) {
    if (waiting[nid] && !(forks[nid] || forks[(nid + 1) % 6])) {
      waiting[nid] = false
      forks[nid] = true
      forks[(nid + 1) % 6] = true

      client.publishEvent("pubsub", "acquire", getMap(nid)).subscribe()
    }
  }

  private fun getMap(i: Int): Map<String, Any> {
    val now = Clock.System.now()
    val epochNanos = (now.epochSeconds * 1_000_000_000L) + now.nanosecondsOfSecond

    return mapOf("id" to i, "time" to epochNanos)
  }

  private fun measureEventTime(data: Map<String, Any>) {
    val now = Clock.System.now()
    val nowNanos = (now.epochSeconds * 1_000_000_000L) + now.nanosecondsOfSecond

    val deltaNanos = (nowNanos - data["time"] as Long).coerceAtLeast(0L)

    metricsRegistry.timer("event.latency")!!.update((deltaNanos), TimeUnit.NANOSECONDS)
  }
}
