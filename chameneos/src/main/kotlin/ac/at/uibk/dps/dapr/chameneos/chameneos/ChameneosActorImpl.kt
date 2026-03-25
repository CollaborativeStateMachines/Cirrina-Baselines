package ac.at.uibk.dps.dapr.chameneos.chameneos

import ac.at.uibk.dps.dapr.chameneos.Chameneos
import io.dapr.actors.ActorId
import io.dapr.actors.runtime.AbstractActor
import io.dapr.actors.runtime.ActorRuntimeContext
import io.dapr.client.DaprClientBuilder
import java.util.concurrent.TimeUnit
import kotlin.random.Random
import kotlin.time.Clock
import kotlin.time.measureTime
import kotlin.time.toJavaDuration

class ChameneosActorImpl(
  runtimeContext: ActorRuntimeContext<ChameneosActorImpl>,
  val actorId: ActorId,
) : AbstractActor(runtimeContext, actorId), ChameneosActor {
  val client = DaprClientBuilder().build()

  var color = Random.nextInt(1, 4)

  var metricRegistry = Chameneos.provideMetricRegistry()

  var eventTimer = metricRegistry.timer("event.latency")
  var requestTimer = metricRegistry.timer("request.duration")
  var meetTimer = metricRegistry.timer("meet.duration")
  var changeTimer = metricRegistry.timer("change.duration")

  override fun request() {
    val delta = measureTime {
      val now = Clock.System.now()
      val epochNanos = (now.epochSeconds * 1_000_000_000L) + now.nanosecondsOfSecond

      client
        .publishEvent(
          "pubsub",
          "request",
          mapOf<String, Any>(
            "requestor" to actorId.toString(),
            "color" to color,
            "time" to epochNanos,
          ),
        )
        .subscribe()
    }
    requestTimer.update(delta.toJavaDuration())
  }

  override fun meet(data: Map<String, Any>) {
    val delta = measureTime {
      val time = data["time"] as Long
      val partnerColor = data["color"] as Int
      val partner = data["partner"] as String

      var now = Clock.System.now()
      var nowNanos = (now.epochSeconds * 1_000_000_000L) + now.nanosecondsOfSecond

      val deltaNanos = (nowNanos - time).coerceAtLeast(0L)

      eventTimer.update(deltaNanos, TimeUnit.NANOSECONDS)

      color = if (color == partnerColor) color else (color xor partnerColor)

      now = Clock.System.now()
      nowNanos = (now.epochSeconds * 1_000_000_000L) + now.nanosecondsOfSecond
      client
        .publishEvent(
          "pubsub",
          "change",
          mapOf<String, Any>("partner" to partner, "color" to color, "time" to nowNanos),
        )
        .subscribe()
    }
    meetTimer.update(delta.toJavaDuration())
    request()
  }

  override fun change(data: Map<String, Any>) {
    val delta = measureTime {
      val time = data["time"] as Long

      val now = Clock.System.now()
      val nowNanos = (now.epochSeconds * 1_000_000_000L) + now.nanosecondsOfSecond

      val deltaNanos = (nowNanos - time).coerceAtLeast(0L)

      eventTimer.update(deltaNanos, TimeUnit.NANOSECONDS)

      color = data["color"] as Int
    }
    changeTimer.update(delta.toJavaDuration())

    request()
  }
}
