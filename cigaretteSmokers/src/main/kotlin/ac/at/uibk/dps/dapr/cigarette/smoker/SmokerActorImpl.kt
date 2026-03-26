package ac.at.uibk.dps.dapr.cigarette.smoker

import ac.at.uibk.dps.dapr.cigarette.CigaretteSmokers
import io.dapr.actors.ActorId
import io.dapr.actors.runtime.AbstractActor
import io.dapr.actors.runtime.ActorRuntimeContext
import io.dapr.client.DaprClient
import io.dapr.client.DaprClientBuilder
import java.security.SecureRandom
import java.util.concurrent.TimeUnit
import kotlin.random.Random
import kotlin.time.Clock

class SmokerActorImpl(runtimeContext: ActorRuntimeContext<SmokerActorImpl>, val actorId: ActorId) :
  AbstractActor(runtimeContext, actorId), SmokerActor {

  val client: DaprClient = DaprClientBuilder().build()

  var metricRegistry = CigaretteSmokers.provideMetricRegistry()

  private val seedGenerator = SecureRandom()
  private val threadRng =
    object : ThreadLocal<Random>() {
      override fun initialValue(): Random {
        return Random(seedGenerator.nextLong())
      }
    }

  override fun smoking(data: Map<String, Any>) {
    var now = Clock.System.now()
    val nowNanos = (now.epochSeconds * 1_000_000_000L) + now.nanosecondsOfSecond
    val deltaNanos = (nowNanos - data["time"] as Long).coerceAtLeast(0L)

    metricRegistry.timer("event.latency").update(deltaNanos, TimeUnit.NANOSECONDS)

    val ingredients = data["ingredients"] as List<*>

    if (!ingredients.contains(actorId)) {
      Thread.sleep(randomAround(10, 2).toLong())

      now = Clock.System.now()
      val epochNanos = (now.epochSeconds * 1_000_000_000L) + now.nanosecondsOfSecond

      client.publishEvent("pubsub", "finish", epochNanos).subscribe()
    }
  }

  fun randomAround(base: Int, delta: Int): Int {
    return (base - delta..base + delta).random(threadRng.get())
  }
}
