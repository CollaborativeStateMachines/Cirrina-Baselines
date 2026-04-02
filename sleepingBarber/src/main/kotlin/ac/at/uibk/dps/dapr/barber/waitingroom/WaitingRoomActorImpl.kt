package ac.at.uibk.dps.dapr.barber.waitingroom

import ac.at.uibk.dps.dapr.barber.SleepingBarber.Companion.metricsRegistry
import io.dapr.actors.ActorId
import io.dapr.actors.runtime.AbstractActor
import io.dapr.actors.runtime.ActorRuntimeContext
import io.dapr.client.DaprClientBuilder
import java.util.concurrent.TimeUnit
import kotlin.time.Clock

class WaitingRoomActorImpl(runtimeContext: ActorRuntimeContext<WaitingRoomActorImpl>, id: ActorId) :
  AbstractActor(runtimeContext, id), WaitingRoomActor {
  val client = DaprClientBuilder().build()

  val waiting: ArrayList<Int> = arrayListOf()
  var isBarberSleeping = true

  override fun enter(data: Map<String, Any>) {
    val now = Clock.System.now()
    val nowNanos = (now.epochSeconds * 1_000_000_000L) + now.nanosecondsOfSecond
    val deltaNanos = (nowNanos - data["time"] as Long).coerceAtLeast(0L)

    metricsRegistry.timer("event.latency").update((deltaNanos), TimeUnit.NANOSECONDS)

    val customer = data["id"].toString().toInt()

    if (waiting.size < 3) {
      waiting.add(customer)
      if (isBarberSleeping) {
        isBarberSleeping = false
        client.publishEvent("pubsub", "sit", getMap(waiting[0])).subscribe()
        waiting.remove(0)
      }
    } else {
      client.publishEvent("pubsub", "full", getMap(customer)).subscribe()
    }
  }

  override fun ready(data: Map<String, Any>) {
    val now = Clock.System.now()
    val nowNanos = (now.epochSeconds * 1_000_000_000L) + now.nanosecondsOfSecond
    val deltaNanos = (nowNanos - data["time"] as Long).coerceAtLeast(0L)

    metricsRegistry.timer("event.latency").update((deltaNanos), TimeUnit.NANOSECONDS)

    isBarberSleeping = true
    if (!waiting.isEmpty()) {
      isBarberSleeping = false
      client.publishEvent("pubsub", "sit", getMap(waiting[0])).subscribe()
      waiting.remove(0)
    }
  }

  private fun getMap(i: Int): Map<String, Any> {
    val now = Clock.System.now()
    val epochNanos = (now.epochSeconds * 1_000_000_000L) + now.nanosecondsOfSecond
    return mapOf("id" to i, "time" to epochNanos)
  }
}
