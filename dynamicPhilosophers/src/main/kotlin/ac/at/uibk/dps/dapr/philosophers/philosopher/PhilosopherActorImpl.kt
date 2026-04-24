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
import java.util.logging.Logger
import kotlin.random.Random
import kotlin.time.Clock

class PhilosopherActorImpl(
  runtimeContext: ActorRuntimeContext<PhilosopherActorImpl>,
  actorId: ActorId,
) : AbstractActor(runtimeContext, actorId), PhilosopherActor {

  companion object {
    private val client: DaprClient = DaprClientBuilder().build()
    private val logger = Logger.getLogger(PhilosopherActorImpl::class.java.name)
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
    logger.info(
      "TRACE: [$id] entry hungry L=$hasLeftFork(d=$leftForkDirty,p=$leftPending) R=$hasRightFork(d=$rightForkDirty,p=$rightPending)"
    )
    evaluateRequests()
  }

  private fun evaluateRequests() {
    logger.info(
      "TRACE: [$id] evaluateRequests L=$hasLeftFork(p=$leftPending) R=$hasRightFork(p=$rightPending)"
    )

    if (leftNeighbor != "none" && !hasLeftFork && !leftRequested) {
      leftRequested = true
      logger.info("TRACE: [$id] SEND requestRightFork -> $leftNeighbor")
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
      logger.info("TRACE: [$id] SEND requestLeftFork -> $rightNeighbor")
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
    logger.info("TRACE: [$id] ate#$meals -> thinking")

    leftForkDirty = true
    rightForkDirty = true

    if (leftNeighbor != "none" && leftPending && hasLeftFork) {
      logger.info("TRACE: [$id] ate#$meals: Fulfilling pending Left request")
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
      logger.info("TRACE: [$id] ate#$meals: Fulfilling pending Right request")
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
    logger.info("TRACE: [$id] RECV giveLeftFork (Clean)")

    when (state) {
      State.INACTIVE -> {
        hasLeftFork = true
        leftForkDirty = false
        leftRequested = false
      }
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
    logger.info("TRACE: [$id] RECV giveRightFork (Clean)")

    when (state) {
      State.INACTIVE -> {
        hasRightFork = true
        rightForkDirty = false
        rightRequested = false
      }
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
      State.INACTIVE -> {
        leftPending = true
      }
      State.HUNGRY -> {
        if (hasLeftFork && leftForkDirty) {
          logger.info(
            "TRACE: [$id] RECV requestLeftFork: Yielding Dirty Left Fork to $leftNeighbor"
          )
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
          logger.info("TRACE: [$id] RECV requestLeftFork: Buffering (Pending=true)")
          leftPending = true
        }
      }
      State.EATING -> {
        logger.info("TRACE: [$id] RECV requestLeftFork (Eating)")
        leftPending = true
      }
      State.THINKING -> {
        if (hasLeftFork) {
          logger.info("TRACE: [$id] RECV requestLeftFork (Thinking): Yielding")
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
          logger.info("TRACE: [$id] RECV requestLeftFork (Thinking): Buffering")
          leftPending = true
        }
      }
    }
  }

  override fun onRequestRightFork(data: Map<String, Any>) {
    recordLatency(data)

    when (state) {
      State.INACTIVE -> {
        rightPending = true
      }
      State.HUNGRY -> {
        if (hasRightFork && rightForkDirty) {
          logger.info(
            "TRACE: [$id] RECV requestRightFork: Yielding Dirty Right Fork to $rightNeighbor"
          )
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
          logger.info("TRACE: [$id] RECV requestRightFork: Buffering (Pending=true)")
          rightPending = true
        }
      }
      State.EATING -> {
        logger.info("TRACE: [$id] RECV requestRightFork (Eating)")
        rightPending = true
      }
      State.THINKING -> {
        if (hasRightFork) {
          logger.info("TRACE: [$id] RECV requestRightFork (Thinking): Yielding")
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
          logger.info("TRACE: [$id] RECV requestRightFork (Thinking): Buffering")
          rightPending = true
        }
      }
    }
  }

  override fun onJoin(data: Map<String, Any>) {
    recordLatency(data)
    val newRightNeighbor = data["id"].toString()
    logger.info("TRACE: [$id] RECV join (${state.name}) rn=$newRightNeighbor")

    rightNeighbor = newRightNeighbor
    hasRightFork = true
    rightForkDirty = true

    if (rightPending) {
      logger.info("TRACE: [$id] Join: Satisfying early buffered request for $rightNeighbor")
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

    if (state == State.HUNGRY) {
      evaluateRequests()
    }
  }

  fun randomAround(base: Int, delta: Int): Int {
    return (base - delta..base + delta).random(threadRng.get())
  }
}
