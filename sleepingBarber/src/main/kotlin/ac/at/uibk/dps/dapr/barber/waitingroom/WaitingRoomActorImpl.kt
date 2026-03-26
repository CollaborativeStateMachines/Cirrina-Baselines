package ac.at.uibk.dps.dapr.barber.waitingroom

import ac.at.uibk.dps.dapr.barber.SleepingBarber.Companion.metricsRegistry
import io.dapr.actors.ActorId
import io.dapr.actors.runtime.AbstractActor
import io.dapr.actors.runtime.ActorRuntimeContext
import io.dapr.client.DaprClient
import io.dapr.client.DaprClientBuilder
import java.util.concurrent.TimeUnit
import kotlin.time.Clock

class WaitingRoomActorImpl(runtimeContext: ActorRuntimeContext<WaitingRoomActorImpl>, id: ActorId) :
  AbstractActor(runtimeContext, id), WaitingRoomActor {
  val client: DaprClient? = DaprClientBuilder().build()

  val waiting: ArrayList<Int> = arrayListOf()

  var barber = "sleeping"

  override fun enter(data: Map<String, Any>) {
    val now = Clock.System.now()
    val nowNanos = (now.epochSeconds * 1_000_000_000L) + now.nanosecondsOfSecond
    val deltaNanos = (nowNanos - data["time"] as Long).coerceAtLeast(0L)
    metricsRegistry.timer("event.latency")!!.update((deltaNanos), TimeUnit.NANOSECONDS)
    val customer = data["id"].toString().toInt()
    if (waiting.size == 3) {
      client!!.publishEvent("pubsub", "full", getMap(customer)).subscribe()
    } else if (barber == "sleeping" && waiting.isEmpty()) {
      client!!.publishEvent("pubsub", "cutting", getMap(customer)).subscribe()
      barber = "busy"
    } else if (barber == "sleeping" && !waiting.isEmpty()) {
      waiting.add(customer)
      client!!.publishEvent("pubsub", "cutting", getMap(waiting[0])).subscribe()
      barber = "busy"
      waiting.removeFirst()
    } else if (barber == "busy" && waiting.size < 3) {
      waiting.add(customer)
    }
  }

  override fun ready(data: Map<String, Any>) {
    val now = Clock.System.now()
    val nowNanos = (now.epochSeconds * 1_000_000_000L) + now.nanosecondsOfSecond
    val deltaNanos = (nowNanos - data["time"] as Long).coerceAtLeast(0L)
    metricsRegistry.timer("event.latency")!!.update((deltaNanos), TimeUnit.NANOSECONDS)
    barber = "sleeping"
    if (!waiting.isEmpty()) {
      barber = "busy"
      client!!.publishEvent("pubsub", "cutting", getMap(waiting[0])).subscribe()
      waiting.removeFirst()
    }
  }

  private fun getMap(i: Int): Map<String, Any> {
    val now = Clock.System.now()
    val epochNanos = (now.epochSeconds * 1_000_000_000L) + now.nanosecondsOfSecond
    return mapOf("id" to i, "time" to epochNanos)
  }
}
