package ac.at.uibk.dps.dapr.count.producer

import io.dapr.actors.ActorId
import io.dapr.actors.runtime.AbstractActor
import io.dapr.actors.runtime.ActorRuntimeContext
import io.dapr.client.DaprClient
import io.dapr.client.DaprClientBuilder
import java.security.SecureRandom
import kotlin.random.Random
import kotlin.time.Clock

class ProducerActorImpl(runtimeContext: ActorRuntimeContext<ProducerActorImpl>, actorId: ActorId) :
  AbstractActor(runtimeContext, actorId), ProducerActor {
  val client: DaprClient = DaprClientBuilder().build()

  private val seedGenerator = SecureRandom()
  private val threadRng =
    object : ThreadLocal<Random>() {
      override fun initialValue(): Random {
        return Random(seedGenerator.nextLong())
      }
    }

  override fun produce() {
    while (true) {
      Thread.sleep(randomAround(10, 2).toLong())

      val now = Clock.System.now()
      val epochNanos = (now.epochSeconds * 1_000_000_000L) + now.nanosecondsOfSecond

      client.publishEvent("pubsub", "increment", epochNanos).subscribe()
    }
  }

  private fun randomAround(base: Int, delta: Int): Int {
    return (base - delta..base + delta).random(threadRng.get())
  }
}
