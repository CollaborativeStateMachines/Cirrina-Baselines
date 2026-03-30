package ac.at.uibk.dps.dapr.barber.customer

import ac.at.uibk.dps.dapr.barber.SleepingBarber
import io.dapr.actors.ActorId
import io.dapr.actors.runtime.AbstractActor
import io.dapr.actors.runtime.ActorRuntimeContext
import io.dapr.client.DaprClient
import io.dapr.client.DaprClientBuilder
import java.security.SecureRandom
import java.util.concurrent.TimeUnit
import kotlin.random.Random
import kotlin.time.Clock

class CustomerActorImpl(runtimeContext: ActorRuntimeContext<CustomerActorImpl>, id: ActorId) :
  AbstractActor(runtimeContext, id), CustomerActor {
  val metricsRegistry = SleepingBarber.metricsRegistry

  val client: DaprClient? = DaprClientBuilder().build()

  private val seedGenerator = SecureRandom()
  private val threadRng =
    object : ThreadLocal<Random>() {
      override fun initialValue(): Random {
        return Random(seedGenerator.nextLong())
      }
    }

  var count = 0

  override fun request() {
    client!!.publishEvent("pubsub", "enter", getMap(id.toString().toInt())).subscribe()
  }

  override fun full(data: Map<String, Any>) {
    measureEventTime(data)
    Thread.sleep(randomAround(10, 2).toLong())
    request()
  }

  override fun comeIn(data: Map<String, Any>) {
    measureEventTime(data)
    ++count
    metricsRegistry.counter("customer.haircuts").inc(1L)
  }

  override fun done(data: Map<String, Any>) {
    measureEventTime(data)
    request()
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

    SleepingBarber.Companion.metricsRegistry
      .timer("event.latency")!!
      .update((deltaNanos), TimeUnit.NANOSECONDS)
  }

  private fun randomAround(base: Int, delta: Int): Int {
    return (base - delta..base + delta).random(threadRng.get())
  }
}
