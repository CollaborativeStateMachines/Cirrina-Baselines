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

  // Moved to companion object to prevent gRPC connection leaks!
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
      override fun initialValue(): Random {
        return Random(seedGenerator.nextLong())
      }
    }

  private val metricsRegistry = DynamicPhilosophers.provideMetricRegistry()

  private var state = State.INACTIVE
  private val id = actorId.toString()
  private var hasLeftFork = true
  private var hasRightFork = true
  private var hasLeftToken = false
  private var hasRightToken = false
  private var leftForkDirty = true
  private var rightForkDirty = true
  private var leftNeighbor = ""
  private var rightNeighbor = ""
  private var hasPendingJoin = false
  private var pendingJoinId = ""

  private var meals: Int = 0

  override fun onInstantiate(data: Map<String, Any>) {
    val now = Clock.System.now()
    val nowNanos = (now.epochSeconds * 1_000_000_000L) + now.nanosecondsOfSecond
    val deltaNanos = (nowNanos - data["time"] as Long).coerceAtLeast(0L)

    metricsRegistry.timer("event.latency")!!.update((deltaNanos), TimeUnit.NANOSECONDS)

    leftNeighbor = data["leftNeighbor"] as String
    rightNeighbor = data["rightNeighbor"] as String

    hungry()
  }

  override fun hungry() {
    state = State.HUNGRY

    if (hasPendingJoin) {
      if (rightNeighbor == "instantiated0" && pendingJoinId != id) {
        rightNeighbor = pendingJoinId
        hasRightFork = false
        hasRightToken = true
        rightForkDirty = true
      }

      if (id == "instantiated0" && pendingJoinId != id) {
        leftNeighbor = pendingJoinId
        hasLeftFork = false
        hasLeftToken = true
        leftForkDirty = true
      }

      hasPendingJoin = false
    }

    evaluateForks()
  }

  private fun evaluateForks() {
    val now = Clock.System.now()
    val epochNanos = (now.epochSeconds * 1_000_000_000L) + now.nanosecondsOfSecond

    if (!hasLeftFork && hasLeftToken) {
      hasLeftToken = false
      client
        .publishEvent(
          "pubsub",
          "requestRightFork",
          mapOf("target" to leftNeighbor, "time" to epochNanos),
        )
        .subscribe()
    }

    if (!hasRightFork && hasRightToken) {
      hasRightToken = false
      client
        .publishEvent(
          "pubsub",
          "requestLeftFork",
          mapOf("target" to rightNeighbor, "time" to epochNanos),
        )
        .subscribe()
    }

    tryEat()
  }

  private fun tryEat() {
    if (hasLeftFork && hasRightFork) {
      eating()
    }
  }

  private fun eating() {
    state = State.EATING
    this.registerActorTimer(
        "eating",
        "ate",
        ByteArray(0),
        Duration.ofMillis(randomAround(10, 2).toLong()),
        Duration.ZERO,
      )
      .subscribe()
  }

  override fun ate() {
    ++meals
    metricsRegistry.counter("philosopher.meals.id=$id").inc()

    leftForkDirty = true
    rightForkDirty = true

    if (hasPendingJoin) {

      if (rightNeighbor == "instantiated0" && pendingJoinId != id) {
        rightNeighbor = pendingJoinId
        hasRightFork = false
        hasRightToken = true
        rightForkDirty = true
      }
      if (id == "instantiated0" && pendingJoinId != id) {
        leftNeighbor = pendingJoinId
        hasLeftFork = false
        hasLeftToken = true
        leftForkDirty = true
      }
      hasPendingJoin = false
    }

    val now = Clock.System.now()
    val epochNanos = (now.epochSeconds * 1_000_000_000L) + now.nanosecondsOfSecond

    if (hasLeftToken && hasLeftFork) {
      hasLeftFork = false

      client
        .publishEvent(
          "pubsub",
          "giveRightFork",
          mapOf("target" to leftNeighbor, "time" to epochNanos),
        )
        .subscribe()
    }
    if (hasRightToken && hasRightFork) {
      hasRightFork = false

      client
        .publishEvent(
          "pubsub",
          "giveLeftFork",
          mapOf("target" to rightNeighbor, "time" to epochNanos),
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
        Duration.ZERO,
      )
      .subscribe()
  }

  override fun thought() {
    hungry()
  }

  override fun onGiveLeftFork(data: Map<String, Any>) {
    val now = Clock.System.now()
    val nowNanos = (now.epochSeconds * 1_000_000_000L) + now.nanosecondsOfSecond
    val deltaNanos = (nowNanos - data["time"] as Long).coerceAtLeast(0L)

    metricsRegistry.timer("event.latency")!!.update((deltaNanos), TimeUnit.NANOSECONDS)

    if (state == State.HUNGRY) {
      hasLeftFork = true
      leftForkDirty = false
      tryEat()
    }
  }

  override fun onGiveRightFork(data: Map<String, Any>) {
    val now = Clock.System.now()
    val nowNanos = (now.epochSeconds * 1_000_000_000L) + now.nanosecondsOfSecond
    val deltaNanos = (nowNanos - data["time"] as Long).coerceAtLeast(0L)

    metricsRegistry.timer("event.latency")!!.update((deltaNanos), TimeUnit.NANOSECONDS)

    if (state == State.HUNGRY) {
      hasRightFork = true
      rightForkDirty = false
      tryEat()
    }
  }

  override fun onRequestLeftFork(data: Map<String, Any>) {
    val now = Clock.System.now()
    val nowNanos = (now.epochSeconds * 1_000_000_000L) + now.nanosecondsOfSecond
    val deltaNanos = (nowNanos - data["time"] as Long).coerceAtLeast(0L)

    metricsRegistry.timer("event.latency")!!.update((deltaNanos), TimeUnit.NANOSECONDS)

    when (state) {
      State.HUNGRY -> {
        hasLeftToken = true
        if (hasLeftFork && leftForkDirty) {
          hasLeftFork = false

          val timeNow = Clock.System.now()
          val epochNanos = (timeNow.epochSeconds * 1_000_000_000L) + timeNow.nanosecondsOfSecond

          client
            .publishEvent(
              "pubsub",
              "giveRightFork",
              mapOf("target" to leftNeighbor, "time" to epochNanos),
            )
            .subscribe()
          hasLeftToken = false

          client
            .publishEvent(
              "pubsub",
              "requestRightFork",
              mapOf("target" to leftNeighbor, "time" to epochNanos),
            )
            .subscribe()
        }
      }
      State.EATING -> {
        hasLeftToken = true
      }
      State.THINKING -> {
        hasLeftToken = true
        if (hasLeftFork && leftForkDirty) {
          hasLeftFork = false
          client
            .publishEvent(
              "pubsub",
              "giveRightFork",
              mapOf("target" to leftNeighbor, "time" to nowNanos),
            )
            .subscribe()
        }
      }
      State.INACTIVE -> {}
    }
  }

  override fun onRequestRightFork(data: Map<String, Any>) {
    val now = Clock.System.now()
    val nowNanos = (now.epochSeconds * 1_000_000_000L) + now.nanosecondsOfSecond
    val deltaNanos = (nowNanos - data["time"] as Long).coerceAtLeast(0L)

    metricsRegistry.timer("event.latency")!!.update((deltaNanos), TimeUnit.NANOSECONDS)

    when (state) {
      State.HUNGRY -> {
        hasRightToken = true
        if (hasRightFork && rightForkDirty) {
          hasRightFork = false

          client
            .publishEvent(
              "pubsub",
              "giveLeftFork",
              mapOf("target" to rightNeighbor, "time" to nowNanos),
            )
            .subscribe()

          hasRightToken = false

          client
            .publishEvent(
              "pubsub",
              "requestLeftFork",
              mapOf("target" to rightNeighbor, "time" to nowNanos),
            )
            .subscribe()
        }
      }
      State.EATING -> {
        hasRightToken = true
      }
      State.THINKING -> {
        hasRightToken = true
        if (hasRightFork && rightForkDirty) {
          hasRightFork = false

          client
            .publishEvent(
              "pubsub",
              "giveLeftFork",
              mapOf("target" to rightNeighbor, "time" to nowNanos),
            )
            .subscribe()
        }
      }
      State.INACTIVE -> {}
    }
  }

  override fun onJoin(data: Map<String, Any>) {
    val now = Clock.System.now()
    val nowNanos = (now.epochSeconds * 1_000_000_000L) + now.nanosecondsOfSecond
    val deltaNanos = (nowNanos - data["time"] as Long).coerceAtLeast(0L)

    metricsRegistry.timer("event.latency")!!.update((deltaNanos), TimeUnit.NANOSECONDS)

    when (state) {
      State.HUNGRY -> {
        if (rightNeighbor == "instantiated0" && id != data["id"]) {
          rightNeighbor = data["id"].toString()
          hasRightFork = false
          hasRightToken = true
          rightForkDirty = true
          evaluateForks()
        }
        if (id == "instantiated0" && id != data["id"]) {
          leftNeighbor = data["id"].toString()
          hasLeftFork = false
          hasLeftToken = true
          leftForkDirty = true
          evaluateForks()
        }
      }
      State.EATING,
      State.THINKING -> {
        hasPendingJoin = true
        pendingJoinId = data["id"].toString()
      }
      State.INACTIVE -> {}
    }
  }

  fun randomAround(base: Int, delta: Int): Int {
    return (base - delta..base + delta).random(threadRng.get())
  }
}
