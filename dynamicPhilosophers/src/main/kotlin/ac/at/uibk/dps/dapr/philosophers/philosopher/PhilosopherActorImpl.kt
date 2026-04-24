package ac.at.uibk.dps.dapr.philosophers.philosopher

import ac.at.uibk.dps.dapr.philosophers.DynamicPhilosophers
import io.dapr.actors.ActorId
import io.dapr.actors.runtime.AbstractActor
import io.dapr.actors.runtime.ActorRuntimeContext
import io.dapr.client.DaprClient
import io.dapr.client.DaprClientBuilder
import java.security.SecureRandom
import java.time.Duration
import java.util.concurrent.TimeUnit
import kotlin.random.Random
import kotlin.time.Clock

class PhilosopherActorImpl(
  runtimeContext: ActorRuntimeContext<PhilosopherActorImpl>,
  actorId: ActorId,
) : AbstractActor(runtimeContext, actorId), PhilosopherActor {

  companion object {
    private val client: DaprClient = DaprClientBuilder().build()
  }

  enum class State {
    INACTIVE,
    HUNGRY,
    EATING,
    THINKING,
  }

  private val seedGenerator = SecureRandom()
  private val threadRng =
    object : ThreadLocal<Random>() {
      override fun initialValue(): Random = Random(seedGenerator.nextLong())
    }

  private val metricsRegistry = DynamicPhilosophers.provideMetricRegistry()

  private var state = State.INACTIVE
  private val id = actorId.toString()

  private var leftNeighbor = "none"
  private var rightNeighbor = "none"
  private var hasLeftFork = false
  private var hasRightFork = false
  private var leftForkDirty = true
  private var rightForkDirty = true
  private var leftRequested = false
  private var rightRequested = false
  private var leftPending = false
  private var rightPending = false
  private var meals: Int = 0

  private fun getNowNanos(): Long {
    val now = Clock.System.now()
    return (now.epochSeconds * 1_000_000_000L) + now.nanosecondsOfSecond
  }

  private fun recordLatency(data: Map<String, Any>) {
    val deltaNanos = (getNowNanos() - (data["time"] as? Long ?: 0L)).coerceAtLeast(0L)
    metricsRegistry.timer("event.latency")!!.update(deltaNanos, TimeUnit.NANOSECONDS)
  }

  override fun onInstantiate(data: Map<String, Any>) {
    recordLatency(data)
    leftNeighbor = data["leftNeighbor"]?.toString() ?: "none"
    rightNeighbor = data["rightNeighbor"]?.toString() ?: "none"

    hasLeftFork = hasLeftFork || (data["hasLeftFork"]?.toString()?.toBooleanStrictOrNull() ?: false)
    hasRightFork =
      hasRightFork || (data["hasRightFork"]?.toString()?.toBooleanStrictOrNull() ?: false)
    leftForkDirty =
      leftForkDirty && (data["leftForkDirty"]?.toString()?.toBooleanStrictOrNull() ?: true)
    rightForkDirty =
      rightForkDirty && (data["rightForkDirty"]?.toString()?.toBooleanStrictOrNull() ?: true)
    leftRequested =
      leftRequested || (data["leftRequested"]?.toString()?.toBooleanStrictOrNull() ?: false)
    rightRequested =
      rightRequested || (data["rightRequested"]?.toString()?.toBooleanStrictOrNull() ?: false)
    leftPending = leftPending || (data["leftPending"]?.toString()?.toBooleanStrictOrNull() ?: false)
    rightPending =
      rightPending || (data["rightPending"]?.toString()?.toBooleanStrictOrNull() ?: false)

    hungry()
  }

  override fun hungry() {
    state = State.HUNGRY
    evaluateRequests()
  }

  private fun evaluateRequests() {
    if (leftNeighbor != "none" && !hasLeftFork && !leftRequested) {
      leftRequested = true
      client
        .publishEvent(
          "pubsub",
          "requestRightFork",
          mapOf("target" to leftNeighbor, "time" to getNowNanos()),
        )
        .subscribe()
    }
    if (rightNeighbor != "none" && !hasRightFork && !rightRequested) {
      rightRequested = true
      client
        .publishEvent(
          "pubsub",
          "requestLeftFork",
          mapOf("target" to rightNeighbor, "time" to getNowNanos()),
        )
        .subscribe()
    }
    tryEat()
  }

  private fun tryEat() {
    if (
      (leftNeighbor == "none" || hasLeftFork) &&
        (rightNeighbor == "none" || hasRightFork) &&
        !(leftNeighbor == "none" && rightNeighbor == "none")
    ) {
      if (state != State.EATING) {
        eating()
      }
    }
  }

  private fun eating() {
    state = State.EATING
    this.registerActorTimer(
        "eating",
        "ate",
        ByteArray(0),
        Duration.ofMillis(randomAround(10, 2).toLong()),
        Duration.ofMillis(-1),
      )
      .subscribe()
  }

  override fun ate() {
    if (this.state != State.EATING) return
    ++meals
    metricsRegistry.counter("philosopher.meals.id=$id").inc()
    leftForkDirty = true
    rightForkDirty = true

    if (leftNeighbor != "none" && leftPending && hasLeftFork) {
      hasLeftFork = false
      leftPending = false
      client
        .publishEvent(
          "pubsub",
          "giveRightFork",
          mapOf("target" to leftNeighbor, "time" to getNowNanos()),
        )
        .subscribe()
    }
    if (rightNeighbor != "none" && rightPending && hasRightFork) {
      hasRightFork = false
      rightPending = false
      client
        .publishEvent(
          "pubsub",
          "giveLeftFork",
          mapOf("target" to rightNeighbor, "time" to getNowNanos()),
        )
        .subscribe()
    }
    thinking()
  }

  private fun thinking() {
    state = State.THINKING
    this.registerActorTimer(
        "thinking",
        "thought",
        ByteArray(0),
        Duration.ofMillis(randomAround(10, 2).toLong()),
        Duration.ofMillis(-1),
      )
      .subscribe()
  }

  override fun thought() {
    if (this.state != State.THINKING) return
    hungry()
  }

  override fun onGiveLeftFork(data: Map<String, Any>) {
    recordLatency(data)
    when (state) {
      State.HUNGRY -> {
        hasLeftFork = true
        leftForkDirty = false
        leftRequested = false
        tryEat()
      }
      State.THINKING -> {
        hasLeftFork = true
        leftForkDirty = false
        leftRequested = false
      }
      else -> {}
    }
  }

  override fun onGiveRightFork(data: Map<String, Any>) {
    recordLatency(data)
    when (state) {
      State.HUNGRY -> {
        hasRightFork = true
        rightForkDirty = false
        rightRequested = false
        tryEat()
      }
      State.THINKING -> {
        hasRightFork = true
        rightForkDirty = false
        rightRequested = false
      }
      else -> {}
    }
  }

  override fun onRequestLeftFork(data: Map<String, Any>) {
    recordLatency(data)
    when (state) {
      State.HUNGRY -> {
        if (hasLeftFork && leftForkDirty) {
          hasLeftFork = false
          leftPending = false
          client
            .publishEvent(
              "pubsub",
              "giveRightFork",
              mapOf("target" to leftNeighbor, "time" to getNowNanos()),
            )
            .subscribe()
          evaluateRequests()
        } else {
          leftPending = true
        }
      }
      State.EATING -> {
        leftPending = true
      }
      State.THINKING -> {
        if (hasLeftFork) {
          hasLeftFork = false
          leftPending = false
          client
            .publishEvent(
              "pubsub",
              "giveRightFork",
              mapOf("target" to leftNeighbor, "time" to getNowNanos()),
            )
            .subscribe()
        } else {
          leftPending = true
        }
      }
      else -> {}
    }
  }

  override fun onRequestRightFork(data: Map<String, Any>) {
    recordLatency(data)
    when (state) {
      State.HUNGRY -> {
        if (hasRightFork && rightForkDirty) {
          hasRightFork = false
          rightPending = false
          client
            .publishEvent(
              "pubsub",
              "giveLeftFork",
              mapOf("target" to rightNeighbor, "time" to getNowNanos()),
            )
            .subscribe()
          evaluateRequests()
        } else {
          rightPending = true
        }
      }
      State.EATING -> {
        rightPending = true
      }
      State.THINKING -> {
        if (hasRightFork) {
          hasRightFork = false
          rightPending = false
          client
            .publishEvent(
              "pubsub",
              "giveLeftFork",
              mapOf("target" to rightNeighbor, "time" to getNowNanos()),
            )
            .subscribe()
        } else {
          rightPending = true
        }
      }
      else -> {}
    }
  }

  override fun onJoin(data: Map<String, Any>) {
    recordLatency(data)
    rightNeighbor = data["id"].toString()
    hasRightFork = true
    rightForkDirty = true

    if (rightPending) {
      hasRightFork = false
      rightPending = false
      client
        .publishEvent(
          "pubsub",
          "giveLeftFork",
          mapOf("target" to rightNeighbor, "time" to getNowNanos()),
        )
        .subscribe()
    }
    if (state == State.HUNGRY) evaluateRequests()
  }

  fun randomAround(base: Int, delta: Int): Int =
    (base - delta..base + delta).random(threadRng.get())
}
