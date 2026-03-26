package ac.at.uibk.dps.dapr.chameneos.chameneos

import ac.at.uibk.dps.dapr.chameneos.Chameneos
import io.dapr.actors.ActorId
import io.dapr.actors.runtime.AbstractActor
import io.dapr.actors.runtime.ActorRuntimeContext
import io.dapr.client.DaprClientBuilder
import java.util.concurrent.TimeUnit
import kotlin.random.Random
import kotlin.time.Clock

class ChameneosActorImpl(
  runtimeContext: ActorRuntimeContext<ChameneosActorImpl>,
  val actorId: ActorId,
) : AbstractActor(runtimeContext, actorId), ChameneosActor {
  val client = DaprClientBuilder().build()

  var metricRegistry = Chameneos.provideMetricRegistry()
  var eventTimer = metricRegistry.timer("event.latency")

  var color = Random.nextInt(1, 4)

  override fun request() {
    val now = Clock.System.now()
    val epochNanos = (now.epochSeconds * 1_000_000_000L) + now.nanosecondsOfSecond

    client
      .publishEvent(
        "pubsub",
        "requesting",
        mapOf<String, Any>("id" to actorId.toString(), "color" to color, "time" to epochNanos),
      )
      .subscribe()
  }

  override fun matchMade(data: Map<String, Any>) {
    val now = Clock.System.now()
    val nowNanos = (now.epochSeconds * 1_000_000_000L) + now.nanosecondsOfSecond
    val deltaNanos = (nowNanos - data["time"] as Long).coerceAtLeast(0L)

    eventTimer.update(deltaNanos, TimeUnit.NANOSECONDS)

    val partnerColor = data["color"] as Int

    color = if (color == partnerColor) color else (color xor partnerColor)

    client
      .publishEvent(
        "pubsub",
        "change",
        mapOf<String, Any>(
          "partner" to data["partner"].toString(),
          "color" to color,
          "time" to nowNanos,
        ),
      )
      .subscribe()

    request()
  }

  override fun change(data: Map<String, Any>) {
    val now = Clock.System.now()
    val nowNanos = (now.epochSeconds * 1_000_000_000L) + now.nanosecondsOfSecond
    val deltaNanos = (nowNanos - data["time"] as Long).coerceAtLeast(0L)

    eventTimer.update(deltaNanos, TimeUnit.NANOSECONDS)

    color = data["color"] as Int
    request()
  }
}
