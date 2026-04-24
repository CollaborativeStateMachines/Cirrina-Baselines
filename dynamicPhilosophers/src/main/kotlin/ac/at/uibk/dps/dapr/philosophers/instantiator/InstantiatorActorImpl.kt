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
  actorId: ActorId,
) : AbstractActor(runtimeContext, actorId), InstantiatorActor {

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

  override fun onNodeCreated(data: Map<String, Any>) {
    val now = Clock.System.now()
    val nowNanos = (now.epochSeconds * 1_000_000_000L) + now.nanosecondsOfSecond
    metricsRegistry
      .timer("event.latency")!!
      .update((nowNanos - (data["time"] as? Long ?: 0L)).coerceAtLeast(0L), TimeUnit.NANOSECONDS)
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
        Duration.ofMillis(5000L),
        Duration.ofMillis(-1),
      )
      .subscribe()
  }

  override fun instantiate() {
    if (this.state != State.INSTANTIATE) return
    val nowNanos =
      (Clock.System.now().epochSeconds * 1_000_000_000L) + Clock.System.now().nanosecondsOfSecond
    val leftNeighbor = if (lastInstantiated == 0) "none" else "instantiated${lastInstantiated - 1}"

    client
      .publishEvent(
        "pubsub",
        "instantiate",
        mapOf(
          "id" to "instantiated$lastInstantiated",
          "leftNeighbor" to leftNeighbor,
          "rightNeighbor" to "none",
          "hasLeftFork" to "false",
          "hasRightFork" to "false",
          "leftForkDirty" to "true",
          "rightForkDirty" to "true",
          "leftRequested" to "false",
          "rightRequested" to "false",
          "leftPending" to "false",
          "rightPending" to "false",
          "time" to nowNanos,
        ),
      )
      .subscribe()

    if (lastInstantiated > 0) {
      client
        .publishEvent(
          "pubsub",
          "join",
          mapOf(
            "id" to "instantiated$lastInstantiated",
            "target" to "instantiated${lastInstantiated - 1}",
            "time" to nowNanos,
          ),
        )
        .subscribe()
    }

    client.publishEvent("pubsub", "nodeCreated", mapOf("time" to nowNanos)).subscribe()
    lastInstantiated += n
    waitTurn()
  }
}
