package ac.at.uibk.dps.dapr.barber.barber

import ac.at.uibk.dps.dapr.barber.SleepingBarber
import io.dapr.actors.ActorId
import io.dapr.actors.runtime.AbstractActor
import io.dapr.actors.runtime.ActorRuntimeContext
import io.dapr.client.DaprClientBuilder
import java.security.SecureRandom
import java.util.concurrent.TimeUnit
import kotlin.random.Random
import kotlin.time.Clock

class BarberActorImpl(runtimeContext: ActorRuntimeContext<BarberActorImpl>, id: ActorId) :
  AbstractActor(runtimeContext, id), BarberActor {
  val client = DaprClientBuilder().build()

  private val seedGenerator = SecureRandom()

  private val threadRng =
    object : ThreadLocal<Random>() {
      override fun initialValue(): Random {
        return Random(seedGenerator.nextLong())
      }
    }

  private val metricsRegistry = SleepingBarber.metricsRegistry

  override fun cutting(data: Map<String, Any>) {
    measureEventTime(data)
    val customer = data["id"].toString().toInt()

    client.publishEvent("pubsub", "comeIn", getMap(customer)).subscribe()

    Thread.sleep(randomAround(10, 2).toLong())

    `continue`(customer)
  }

  private fun `continue`(customer: Int) {
    client.publishEvent("pubsub", "done", getMap(customer)).subscribe()
    client.publishEvent("pubsub", "ready", getMap()).subscribe()
  }

  fun randomAround(base: Int, delta: Int): Int {
    return (base - delta..base + delta).random(threadRng.get())
  }

  private fun getMap(): Map<String, Any> {
    val now = Clock.System.now()
    val epochNanos = (now.epochSeconds * 1_000_000_000L) + now.nanosecondsOfSecond

    return mapOf("time" to epochNanos)
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

    metricsRegistry.timer("event.latency").update((deltaNanos), TimeUnit.NANOSECONDS)
  }
}
