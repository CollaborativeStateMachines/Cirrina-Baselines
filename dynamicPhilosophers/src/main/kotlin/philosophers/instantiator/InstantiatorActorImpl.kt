package ac.at.uibk.dps.dapr.philosophers.instantiator

import ac.at.uibk.dps.dapr.philosophers.DynamicPhilosophers
import io.dapr.actors.ActorId
import io.dapr.actors.runtime.AbstractActor
import io.dapr.actors.runtime.ActorRuntimeContext
import io.dapr.client.DaprClient
import io.dapr.client.DaprClientBuilder
import java.time.Duration
import java.util.concurrent.TimeUnit
import kotlin.time.Clock

class InstantiatorActorImpl(
  runtimeContext: ActorRuntimeContext<InstantiatorActorImpl>,
  id: ActorId,
) : AbstractActor(runtimeContext, id), InstantiatorActor {

  enum class State {
    WAIT,
    INSTANTIATE,
  }

  private val client: DaprClient = DaprClientBuilder().build()

  val metricsRegistry = DynamicPhilosophers.provideMetricRegistry()

  private var state = State.WAIT
  val n = 12
  val id = System.getenv("RUNTIME_ID").toInt()
  var count = System.getenv("RUNTIME_ID").toInt()
  var lastInstantiated = System.getenv("RUNTIME_ID").toInt()

  override fun waitTurn() {
    state = State.WAIT
    if (count == 0) {
      count = n
      timer()
    }
  }

  override fun onJoin(data: Map<String, Any>) {
    val now = Clock.System.now()
    val nowNanos = (now.epochSeconds * 1_000_000_000L) + now.nanosecondsOfSecond
    val deltaNanos = (nowNanos - data["time"] as Long).coerceAtLeast(0L)

    metricsRegistry.timer("event.latency")!!.update((deltaNanos), TimeUnit.NANOSECONDS)

    if (state == State.WAIT) {
      --count
      waitTurn()
    }
  }

  private fun timer() {
    state = State.INSTANTIATE
    this.registerActorTimer(
        "instantiate",
        "instantiate",
        ByteArray(0),
        Duration.ofMillis(10000L),
        Duration.ZERO,
      )
      .subscribe()
  }

  override fun instantiate() {
    val now = Clock.System.now()
    val nowNanos = (now.epochSeconds * 1_000_000_000L) + now.nanosecondsOfSecond

    val leftNeighbor =
      if (lastInstantiated == 0) "instantiated0" else "instantiated${lastInstantiated-1}"
    client
      .publishEvent(
        "pubsub",
        "instantiate",
        mapOf(
          "id" to "instantiated$lastInstantiated",
          "leftNeighbor" to leftNeighbor,
          "rightNeighbor" to "instantiated0",
          "time" to nowNanos,
        ),
      )
      .subscribe()
    client
      .publishEvent(
        "pubsub",
        "join",
        mapOf("id" to "instantiated$lastInstantiated", "time" to nowNanos),
      )
      .subscribe()
    lastInstantiated += n

    waitTurn()
  }
}
