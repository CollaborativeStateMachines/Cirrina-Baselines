package ac.at.uibk.dps.dapr.philosophers.instantiator

import ac.at.uibk.dps.dapr.philosophers.DynamicPhilosophers
import io.dapr.actors.ActorId
import io.dapr.actors.runtime.AbstractActor
import io.dapr.actors.runtime.ActorRuntimeContext
import io.dapr.client.DaprClient
import io.dapr.client.DaprClientBuilder
import java.time.Duration

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
        Duration.ZERO,
      )
      .subscribe()
  }

  override fun instantiate() {
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
        ),
      )
      .subscribe()
    client
      .publishEvent("pubsub", "join", mapOf("id" to "instantiated$lastInstantiated"))
      .subscribe()
    lastInstantiated += n

    waitTurn()
  }
}
