package ac.at.uibk.dps.dapr.philosophers.philosopher

import ac.at.uibk.dps.dapr.philosophers.DiningPhilosophers
import io.dapr.actors.ActorId
import io.dapr.actors.runtime.AbstractActor
import io.dapr.actors.runtime.ActorRuntimeContext
import io.dapr.client.DaprClient
import io.dapr.client.DaprClientBuilder
import java.security.SecureRandom
import java.util.concurrent.TimeUnit
import kotlin.random.Random
import kotlin.time.Clock

class PhilosopherActorImpl(runtimeContext: ActorRuntimeContext<PhilosopherActorImpl>, id: ActorId) :
  AbstractActor(runtimeContext, id), PhilosopherActor {
  private val client: DaprClient = DaprClientBuilder().build()

  private val seedGenerator = SecureRandom()
  private val threadRng =
    object : ThreadLocal<Random>() {
      override fun initialValue(): Random {
        return Random(seedGenerator.nextLong())
      }
    }

  private val metricsRegistry = DiningPhilosophers.provideMetricRegistry()

  private var meals: Int = 0

  override fun starting() {
    client.publishEvent("pubsub", "hungry", getMap()).subscribe()
  }

  override fun eating(data: Map<String, Any>) {
    val now = Clock.System.now()
    val nowNanos = (now.epochSeconds * 1_000_000_000L) + now.nanosecondsOfSecond
    val deltaNanos = (nowNanos - data["time"] as Long).coerceAtLeast(0L)

    metricsRegistry.timer("event.latency")!!.update((deltaNanos), TimeUnit.NANOSECONDS)

    Thread.sleep(randomAround(10, 2).toLong())

    ++meals
    metricsRegistry.counter("philosopher.meals").inc(1L)

    client.publishEvent("pubsub", "release", getMap()).subscribe()
    client.publishEvent("pubsub", "hungry", getMap()).subscribe()
  }

  private fun getMap(): Map<String, Any> {
    val now = Clock.System.now()
    val epochNanos = (now.epochSeconds * 1_000_000_000L) + now.nanosecondsOfSecond

    return mapOf("id" to id.toString(), "time" to epochNanos)
  }

  fun randomAround(base: Int, delta: Int): Int {
    return (base - delta..base + delta).random(threadRng.get())
  }
}
