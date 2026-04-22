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
import reactor.core.publisher.Mono

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
  private val numericId = id.filter { it.isDigit() }

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

  private fun stateSnapshot(): String {
    return "L=$hasLeftFork(d=$leftForkDirty,r=$leftRequested,p=$leftPending) R=$hasRightFork(d=$rightForkDirty,r=$rightRequested,p=$rightPending) ln=$leftNeighbor rn=$rightNeighbor"
  }

  override fun onInstantiate(data: Map<String, Any>) {
    recordLatency(data)
    leftNeighbor = data["leftNeighbor"]?.toString() ?: "none"
    rightNeighbor = data["rightNeighbor"]?.toString() ?: "none"

    // MERGE incoming data with any state buffered from early messages
    hasLeftFork = hasLeftFork || (data["hasLeftFork"]?.toString()?.toBooleanStrictOrNull() ?: false)
    hasRightFork = hasRightFork || (data["hasRightFork"]?.toString()?.toBooleanStrictOrNull() ?: false)

    // Dirty flags default to true. If an early fork arrived clean (false), keep it clean (false) using AND.
    leftForkDirty = leftForkDirty && (data["leftForkDirty"]?.toString()?.toBooleanStrictOrNull() ?: true)
    rightForkDirty = rightForkDirty && (data["rightForkDirty"]?.toString()?.toBooleanStrictOrNull() ?: true)

    leftRequested = leftRequested || (data["leftRequested"]?.toString()?.toBooleanStrictOrNull() ?: false)
    rightRequested = rightRequested || (data["rightRequested"]?.toString()?.toBooleanStrictOrNull() ?: false)
    leftPending = leftPending || (data["leftPending"]?.toString()?.toBooleanStrictOrNull() ?: false)
    rightPending = rightPending || (data["rightPending"]?.toString()?.toBooleanStrictOrNull() ?: false)

    println("TRACE: [Node $numericId] onInstantiate ${stateSnapshot()}")
    hungry()
  }

  override fun hungry() {
    state = State.HUNGRY
    evaluateRequests()
  }

  private fun evaluateRequests() {
    if (leftNeighbor != "none" && !hasLeftFork && !leftRequested) {
      leftRequested = true
      println("TRACE: [Node $numericId] SEND requestRightFork -> $leftNeighbor")
      client
        .publishEvent(
          "pubsub",
          "requestRightFork",
          mapOf("target" to leftNeighbor, "time" to getNowNanos()),
        )
        .subscribeSafely("publish requestRightFork to $leftNeighbor")
    }
    if (rightNeighbor != "none" && !hasRightFork && !rightRequested) {
      rightRequested = true
      println("TRACE: [Node $numericId] SEND requestLeftFork -> $rightNeighbor")
      client
        .publishEvent(
          "pubsub",
          "requestLeftFork",
          mapOf("target" to rightNeighbor, "time" to getNowNanos()),
        )
        .subscribeSafely("publish requestLeftFork to $rightNeighbor")
    }
    tryEat()
  }

  private fun tryEat() {
    if ((leftNeighbor == "none" || hasLeftFork) && (rightNeighbor == "none" || hasRightFork)) {
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
      .subscribeSafely("register eating timer")
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
      println("TRACE: [Node $numericId] ate#$meals SEND giveRightFork -> $leftNeighbor")
      client
        .publishEvent(
          "pubsub",
          "giveRightFork",
          mapOf("target" to leftNeighbor, "time" to getNowNanos()),
        )
        .subscribeSafely("publish giveRightFork to $leftNeighbor")
    }

    if (rightNeighbor != "none" && rightPending && hasRightFork) {
      hasRightFork = false
      rightPending = false
      println("TRACE: [Node $numericId] ate#$meals SEND giveLeftFork -> $rightNeighbor")
      client
        .publishEvent(
          "pubsub",
          "giveLeftFork",
          mapOf("target" to rightNeighbor, "time" to getNowNanos()),
        )
        .subscribeSafely("publish giveLeftFork to $rightNeighbor")
    }

    println("TRACE: [Node $numericId] ate#$meals -> thinking ${stateSnapshot()}")
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
      .subscribeSafely("register thinking timer")
  }

  override fun thought() {
    if (this.state != State.THINKING) return
    hungry()
  }

  override fun onGiveLeftFork(data: Map<String, Any>) {
    recordLatency(data)
    println("TRACE: [Node $numericId] RECV giveLeftFork state=$state ${stateSnapshot()}")
    when (state) {
      State.INACTIVE -> {
        println("TRACE: [Node $numericId] giveLeftFork INACTIVE -> buffering early fork")
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
    println("TRACE: [Node $numericId] RECV giveRightFork state=$state ${stateSnapshot()}")
    when (state) {
      State.INACTIVE -> {
        println("TRACE: [Node $numericId] giveRightFork INACTIVE -> buffering early fork")
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
    println("TRACE: [Node $numericId] RECV requestLeftFork state=$state ${stateSnapshot()}")
    when (state) {
      State.INACTIVE -> {
        println("TRACE: [Node $numericId] requestLeftFork INACTIVE -> buffering early request")
        leftPending = true
      }
      State.HUNGRY -> {
        if (!hasLeftFork) {
          leftPending = true
          println("TRACE: [Node $numericId] requestLeftFork HUNGRY no fork -> pending")
        }
        if (hasLeftFork && !leftForkDirty) {
          leftPending = true
          println("TRACE: [Node $numericId] requestLeftFork HUNGRY clean -> pending")
        }
        if (hasLeftFork && leftForkDirty) {
          hasLeftFork = false
          println(
            "TRACE: [Node $numericId] requestLeftFork HUNGRY dirty -> SEND giveRightFork -> $leftNeighbor"
          )
          client
            .publishEvent(
              "pubsub",
              "giveRightFork",
              mapOf("target" to leftNeighbor, "time" to getNowNanos()),
            )
            .subscribeSafely("publish giveRightFork")
          evaluateRequests()
        }
      }
      State.EATING -> {
        leftPending = true
      }
      State.THINKING -> {
        if (!hasLeftFork) {
          leftPending = true
        }
        if (hasLeftFork && !leftForkDirty) {
          leftPending = true
        }
        if (hasLeftFork && leftForkDirty) {
          hasLeftFork = false
          println(
            "TRACE: [Node $numericId] requestLeftFork THINKING dirty -> SEND giveRightFork -> $leftNeighbor"
          )
          client
            .publishEvent(
              "pubsub",
              "giveRightFork",
              mapOf("target" to leftNeighbor, "time" to getNowNanos()),
            )
            .subscribeSafely("publish giveRightFork")
        }
      }
    }
  }

  override fun onRequestRightFork(data: Map<String, Any>) {
    recordLatency(data)
    println("TRACE: [Node $numericId] RECV requestRightFork state=$state ${stateSnapshot()}")
    when (state) {
      State.INACTIVE -> {
        println("TRACE: [Node $numericId] requestRightFork INACTIVE -> buffering early request")
        rightPending = true
      }
      State.HUNGRY -> {
        if (!hasRightFork) {
          rightPending = true
          println("TRACE: [Node $numericId] requestRightFork HUNGRY no fork -> pending")
        }
        if (hasRightFork && !rightForkDirty) {
          rightPending = true
          println("TRACE: [Node $numericId] requestRightFork HUNGRY clean -> pending")
        }
        if (hasRightFork && rightForkDirty) {
          hasRightFork = false
          println(
            "TRACE: [Node $numericId] requestRightFork HUNGRY dirty -> SEND giveLeftFork -> $rightNeighbor"
          )
          client
            .publishEvent(
              "pubsub",
              "giveLeftFork",
              mapOf("target" to rightNeighbor, "time" to getNowNanos()),
            )
            .subscribeSafely("publish giveLeftFork")
          evaluateRequests()
        }
      }
      State.EATING -> {
        rightPending = true
      }
      State.THINKING -> {
        if (!hasRightFork) {
          rightPending = true
        }
        if (hasRightFork && !rightForkDirty) {
          rightPending = true
        }
        if (hasRightFork && rightForkDirty) {
          hasRightFork = false
          println(
            "TRACE: [Node $numericId] requestRightFork THINKING dirty -> SEND giveLeftFork -> $rightNeighbor"
          )
          client
            .publishEvent(
              "pubsub",
              "giveLeftFork",
              mapOf("target" to rightNeighbor, "time" to getNowNanos()),
            )
            .subscribeSafely("publish giveLeftFork")
        }
      }
    }
  }

  override fun onJoin(data: Map<String, Any>) {
    recordLatency(data)
    val newRightNeighbor = data["id"].toString()
    println(
      "TRACE: [Node $numericId] RECV join newRight=$newRightNeighbor state=$state ${stateSnapshot()}"
    )
    rightNeighbor = newRightNeighbor
    hasRightFork = false
    rightRequested = false
    rightPending = false
    if (state == State.HUNGRY) {
      evaluateRequests()
    }
  }

  private fun <T> Mono<T>.subscribeSafely(actionName: String) {
    this.doOnError { e ->
      println("WARN: [Node $numericId] $actionName error, retrying: ${e.message}")
    }
      .retry(3) // Retry up to 3 times for transient sidecar issues
      .subscribe(
        { /* Success - no operation needed for Mono<Void> */ },
        { e -> println("FATAL: [Node $numericId] $actionName failed permanently: ${e.message}") },
      )
  }

  fun randomAround(base: Int, delta: Int): Int {
    return (base - delta..base + delta).random(threadRng.get())
  }
}