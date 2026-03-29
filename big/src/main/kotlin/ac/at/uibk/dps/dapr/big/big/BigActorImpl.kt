package ac.at.uibk.dps.dapr.big.big

import ac.at.uibk.dps.dapr.big.Big
import io.dapr.actors.ActorId
import io.dapr.actors.runtime.AbstractActor
import io.dapr.actors.runtime.ActorRuntimeContext
import io.dapr.client.DaprClientBuilder
import java.util.concurrent.TimeUnit
import kotlin.time.Clock

class BigActorImpl(runtimeContext: ActorRuntimeContext<BigActorImpl>, val actorId: ActorId) :
  AbstractActor(runtimeContext, actorId), BigActor {
  val client = DaprClientBuilder().build()

  var metricRegistry = Big.provideMetricRegistry()

  var counter = metricRegistry.counter("big.pings")
  var eventTimer = metricRegistry.timer("event.latency")

  private val neighbors: IntArray = (0..11).filter { it != actorId.toString().toInt() }.toIntArray()
  private var count = 0

  override fun register() {
    client.publishEvent("pubsub", "register", actorId.toString()).subscribe()
  }

  override fun initial() {
    ++count
    counter.inc()

    val now = Clock.System.now()
    val epochNanos = (now.epochSeconds * 1_000_000_000L) + now.nanosecondsOfSecond

    client
      .publishEvent(
        "pubsub",
        "ping",
        mapOf("sender" to actorId.toString(), "target" to neighbors.random(), "time" to epochNanos),
      )
      .subscribe()
  }

  override fun onPing(data: Map<String, Any>) {
    val now = Clock.System.now()
    val nowNanos = (now.epochSeconds * 1_000_000_000L) + now.nanosecondsOfSecond
    val deltaNanos = (nowNanos - data["time"] as Long).coerceAtLeast(0L)

    eventTimer.update(deltaNanos, TimeUnit.NANOSECONDS)

    client
      .publishEvent(
        "pubsub",
        "pong",
        mapOf("sender" to actorId.toString(), "target" to data["sender"], "time" to nowNanos),
      )
      .subscribe()
  }

  override fun onPong(data: Map<String, Any>) {
    val time = data["time"] as Long

    val now = Clock.System.now()
    val nowNanos = (now.epochSeconds * 1_000_000_000L) + now.nanosecondsOfSecond
    val deltaNanos = (nowNanos - time).coerceAtLeast(0L)

    eventTimer.update(deltaNanos, TimeUnit.NANOSECONDS)

    if (count % 1000 == 0) {
      client.publishEvent("pubsub", "report", "time" to nowNanos).subscribe()
    }
    ++count
    counter.inc()

    client
      .publishEvent(
        "pubsub",
        "ping",
        mapOf("sender" to actorId.toString(), "target" to neighbors.random(), "time" to nowNanos),
      )
      .subscribe()
  }
}
