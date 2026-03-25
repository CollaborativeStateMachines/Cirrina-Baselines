package ac.at.uibk.dps.dapr.big.big

// import io.dropwizard.metrics5.CsvReporter
// import io.dropwizard.metrics5.MetricName
// import io.dropwizard.metrics5.MetricRegistry
import ac.at.uibk.dps.dapr.big.Big
import io.dapr.actors.ActorId
import io.dapr.actors.runtime.AbstractActor
import io.dapr.actors.runtime.ActorRuntimeContext
import io.dapr.client.DaprClientBuilder
import java.util.concurrent.TimeUnit
import kotlin.time.Clock
import kotlin.time.measureTime
import kotlin.time.toJavaDuration

class BigActorImpl(runtimeContext: ActorRuntimeContext<BigActorImpl>, val actorId: ActorId) :
  AbstractActor(runtimeContext, actorId), BigActor {
  val client = DaprClientBuilder().build()

  var neighbors = emptyList<String>()
  var target: String = ""
  var count = 0

  var metricRegistry = Big.provideMetricRegistry()

  var counter = metricRegistry.counter("big.pings")
  var eventTimer = metricRegistry.timer("event.latency")
  var pingTimer = metricRegistry.timer("ping.duration")
  var pongTimer = metricRegistry.timer("pong.duration")

  override fun register() {
    client.publishEvent("pubsub", "register", actorId.toString()).subscribe()
  }

  override fun assignNeighbors(neighbors: List<String>) {
    this.neighbors = neighbors
    sendPing()
  }

  override fun receivePong(data: Map<String, Any>) {
    val delta = measureTime {
      val time = data["time"] as Long

      val now = Clock.System.now()
      val nowNanos = (now.epochSeconds * 1_000_000_000L) + now.nanosecondsOfSecond
      val deltaNanos = (nowNanos - time).coerceAtLeast(0L)

      eventTimer.update(deltaNanos, TimeUnit.NANOSECONDS)

      val sender = data["sender"] as String

      if (sender == target) {
        sendPing()
        ++count

        counter.inc()
      }
    }
    pingTimer.update(delta.toJavaDuration())
  }

  override fun sendPong(data: Map<String, Any>) {
    val delta = measureTime {
      val time = data["time"] as Long

      val now = Clock.System.now()
      val nowNanos = (now.epochSeconds * 1_000_000_000L) + now.nanosecondsOfSecond
      val deltaNanos = (nowNanos - time).coerceAtLeast(0L)

      eventTimer.update(deltaNanos, TimeUnit.NANOSECONDS)

      val sender = data["sender"] as String

      client
        .publishEvent(
          "pubsub",
          "pong",
          mapOf("sender" to actorId.toString(), "receiver" to sender, "time" to nowNanos),
        )
        .subscribe()
    }
    pongTimer.update(delta.toJavaDuration())
  }

  private fun sendPing() {
    target = neighbors.random()
    val now = Clock.System.now()
    val nowNanos = (now.epochSeconds * 1_000_000_000L) + now.nanosecondsOfSecond

    client
      .publishEvent(
        "pubsub",
        "ping",
        mapOf("sender" to actorId.toString(), "receiver" to target, "time" to nowNanos),
      )
      .subscribe()
  }
}
