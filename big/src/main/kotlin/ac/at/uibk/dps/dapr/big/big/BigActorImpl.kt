package ac.at.uibk.dps.dapr.big.big

import io.dapr.actors.ActorId
import io.dapr.actors.runtime.AbstractActor
import io.dapr.actors.runtime.ActorRuntimeContext
import io.dapr.client.DaprClientBuilder
import io.dropwizard.metrics5.CsvReporter
import io.dropwizard.metrics5.MetricName
import io.dropwizard.metrics5.MetricRegistry
import java.nio.file.Paths
import java.util.concurrent.TimeUnit
import kotlin.time.measureTime
import kotlin.time.toJavaDuration

class BigActorImpl(runtimeContext: ActorRuntimeContext<BigActorImpl>, val actorId: ActorId) :
    AbstractActor(runtimeContext, actorId), BigActor {
  val client = DaprClientBuilder().build()

  var neighbors = emptyList<String>()
  var target: String = ""
  var count = 0

  var metricRegistry: MetricRegistry =
      MetricRegistry().apply {
        CsvReporter.forRegistry(this)
            .build(Paths.get("/metrics").toAbsolutePath().toFile())
            .start(1L, TimeUnit.SECONDS)
      }

  var counter =
      metricRegistry.counter(
          MetricName.build("big.pings").tagged(mapOf("id" to actorId.toString()))
      )
  var eventTimer =
      metricRegistry.timer(
          MetricName.build("event.latency").tagged(mapOf("id" to actorId.toString()))
      )
  var pingTimer =
      metricRegistry.timer(MetricName.build("ping.duration").tagged("id", actorId.toString()))
  var pongTimer =
      metricRegistry.timer(MetricName.build("pong.duration").tagged("id", actorId.toString()))

  override fun register() {
    client
        .publishEvent(
            "pubsub",
            "register",
            actorId.toString(),
        )
        .subscribe()
  }

  override fun assignNeighbors(neighbors: List<String>) {
    this.neighbors = neighbors
    sendPing()
  }

  override fun receivePong(data: Map<String, Any>) {
    val delta = measureTime {
      val time = data["time"] as Long
      eventTimer.update((System.nanoTime() - time) / 1_000, TimeUnit.MICROSECONDS)

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
      eventTimer.update((System.nanoTime() - time) / 1_000, TimeUnit.MICROSECONDS)

      val sender = data["sender"] as String

      client
          .publishEvent(
              "pubsub",
              "pong",
              mapOf(
                  "sender" to actorId.toString(),
                  "receiver" to sender,
                  "time" to System.nanoTime(),
              ),
          )
          .subscribe()
    }
    pongTimer.update(delta.toJavaDuration())
  }

  private fun sendPing() {
    target = neighbors.random()
    client
        .publishEvent(
            "pubsub",
            "ping",
            mapOf(
                "sender" to actorId.toString(),
                "receiver" to target,
                "time" to System.nanoTime(),
            ),
        )
        .subscribe()
  }
}
