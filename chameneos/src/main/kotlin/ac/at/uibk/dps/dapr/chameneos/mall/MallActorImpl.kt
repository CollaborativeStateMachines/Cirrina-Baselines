package ac.at.uibk.dps.dapr.chameneos.mall

import ac.at.uibk.dps.dapr.chameneos.Chameneos
import io.dapr.actors.ActorId
import io.dapr.actors.runtime.AbstractActor
import io.dapr.actors.runtime.ActorRuntimeContext
import io.dapr.client.DaprClientBuilder
import java.util.concurrent.TimeUnit
import kotlin.time.Clock

class MallActorImpl(runtimeContext: ActorRuntimeContext<MallActorImpl>, actorId: ActorId) :
  AbstractActor(runtimeContext, actorId), MallActor {

  val client = DaprClientBuilder().build()

  var metricRegistry = Chameneos.provideMetricRegistry()

  var count = 0
  var waiting: ArrayList<String> = arrayListOf()

  override fun requesting(data: Map<String, Any>) {
    var now = Clock.System.now()
    var nowNanos = (now.epochSeconds * 1_000_000_000L) + now.nanosecondsOfSecond
    val deltaNanos = (nowNanos - data["time"] as Long).coerceAtLeast(0L)

    metricRegistry.timer("event.latency").update(deltaNanos, TimeUnit.NANOSECONDS)

    val id = data["id"] as String

    if (waiting.isEmpty()) {
      waiting.add(id)
    } else {
      ++count
      metricRegistry.counter("mall.meetings").inc()

      now = Clock.System.now()
      nowNanos = (now.epochSeconds * 1_000_000_000L) + now.nanosecondsOfSecond

      client
        .publishEvent(
          "pubsub",
          "matchMade",
          mapOf<String, Any>(
            "target" to waiting[0],
            "partner" to id,
            "color" to data["color"] as Int,
            "time" to nowNanos,
          ),
        )
        .subscribe()

      waiting.removeAt(0)
    }
  }
}
