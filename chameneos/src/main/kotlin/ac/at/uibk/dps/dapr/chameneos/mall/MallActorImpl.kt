package ac.at.uibk.dps.dapr.chameneos.mall

import ac.at.uibk.dps.dapr.chameneos.Chameneos
import io.dapr.actors.ActorId
import io.dapr.actors.runtime.AbstractActor
import io.dapr.actors.runtime.ActorRuntimeContext
import io.dapr.client.DaprClientBuilder
import java.util.concurrent.TimeUnit
import kotlin.time.Clock
import kotlin.time.measureTime
import kotlin.time.toJavaDuration

class MallActorImpl(runtimeContext: ActorRuntimeContext<MallActorImpl>, actorId: ActorId) :
  AbstractActor(runtimeContext, actorId), MallActor {

  val client = DaprClientBuilder().build()

  var count = 0
  var startTime = System.nanoTime()
  var waiting: MutableList<String> = mutableListOf()

  var metricRegistry = Chameneos.provideMetricRegistry()
  var counter = metricRegistry.counter("mall.meetings")
  var eventTimer = metricRegistry.timer("event.latency")
  var requestingTimer = metricRegistry.timer("request.duration")

  override fun requesting(data: Map<String, Any>) {
    val delta = measureTime {
      val time = data["time"] as Long
      val requestor = data["requestor"] as String
      val requestorColor = data["color"] as Int

      var now = Clock.System.now()
      var nowNanos = (now.epochSeconds * 1_000_000_000L) + now.nanosecondsOfSecond

      val deltaNanos = (nowNanos - time).coerceAtLeast(0L)

      eventTimer.update(deltaNanos, TimeUnit.NANOSECONDS)

      if (count == 0) startTime = System.nanoTime()

      if (waiting.isEmpty()) {
        waiting.add(requestor)
      } else {
        count++
        counter.inc()

        val waitingId = waiting.removeAt(0)

        now = Clock.System.now()
        nowNanos = (now.epochSeconds * 1_000_000_000L) + now.nanosecondsOfSecond

        client
          .publishEvent(
            "pubsub",
            "meet",
            mapOf<String, Any>(
              "initiator" to waitingId,
              "partner" to requestor,
              "color" to requestorColor,
              "time" to nowNanos,
            ),
          )
          .subscribe()
      }
    }
    requestingTimer.update(delta.toJavaDuration())
  }
}
