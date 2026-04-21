package ac.at.uibk.dps.dapr.philosophers.philosopher

import ac.at.uibk.dps.dapr.philosophers.DynamicPhilosophers
import io.dapr.actors.ActorId
import io.dapr.actors.runtime.AbstractActor
import io.dapr.actors.runtime.ActorRuntimeContext
import io.dapr.client.DaprClient
import io.dapr.client.DaprClientBuilder
import java.security.SecureRandom
import java.time.Duration
import kotlin.random.Random

class PhilosopherActorImpl(
  runtimeContext: ActorRuntimeContext<PhilosopherActorImpl>,
  actorId: ActorId,
) : AbstractActor(runtimeContext, actorId), PhilosopherActor {

  enum class State {
    HUNGRY,
    EATING,
    THINKING,
  }

  private val client: DaprClient = DaprClientBuilder().build()

  private val seedGenerator = SecureRandom()
  private val threadRng =
    object : ThreadLocal<Random>() {
      override fun initialValue(): Random {
        return Random(seedGenerator.nextLong())
      }
    }

  private val metricsRegistry = DynamicPhilosophers.provideMetricRegistry()

  private var state = State.HUNGRY
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
    leftNeighbor = data["leftNeighbor"] as String
    rightNeighbor = data["rightNeighbor"] as String
    hungry()
  }

  override fun hungry() {
    state = State.HUNGRY
    println("$id is hungry")
    println("$id Tokens: $hasLeftToken $hasRightToken")
    println("$id Forks: $hasLeftFork $hasRightFork")
    println("$id Dirty: $leftForkDirty $rightForkDirty")

    if (hasPendingJoin) {
      println("$id processing pending join for $pendingJoinId")
      if (rightNeighbor == "instantiated0" && pendingJoinId != id) {
        println("$id updating rightNeighbor to $pendingJoinId")
        rightNeighbor = pendingJoinId
        hasRightFork = false
        hasRightToken = true
        rightForkDirty = true
      }
      if (id == "instantiated0" && pendingJoinId != id) {
        println("$id updating leftNeighbor to $leftNeighbor")
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
    if (!hasLeftFork && hasLeftToken) {
      println("$id requesting left fork from $leftNeighbor")
      hasLeftToken = false
      client.publishEvent("pubsub", "requestRightFork", mapOf("target" to leftNeighbor)).subscribe()
    }
    if (!hasRightFork && hasRightToken) {
      println("$id requesting right fork from $rightNeighbor")
      hasRightToken = false
      client.publishEvent("pubsub", "requestLeftFork", mapOf("target" to rightNeighbor)).subscribe()
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
    println("$id EATING (meals so far: $meals)")
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
      println("$id processing pending join for $pendingJoinId")
      if (rightNeighbor == "instantiated0" && pendingJoinId != id) {
        println("$id updating rightNeighbor to $pendingJoinId")
        rightNeighbor = pendingJoinId
        hasRightFork = false
        hasRightToken = true
        rightForkDirty = true
      }
      if (id == "instantiated0" && pendingJoinId != id) {
        println("$id updating leftNeighbor to $leftNeighbor")
        leftNeighbor = pendingJoinId
        hasLeftFork = false
        hasLeftToken = true
        leftForkDirty = true
      }
      hasPendingJoin = false
    }

    println("$id done eating, deffered L= $hasLeftToken R= $hasRightToken")
    if (hasLeftToken && hasLeftFork) {
      println("$id fulfilling deferred LEFT to $leftNeighbor")
      hasLeftFork = false
      client.publishEvent("pubsub", "giveRightFork", mapOf("target" to leftNeighbor)).subscribe()
    }
    if (hasRightToken && hasRightFork) {
      println("$id fulfilling deferred RIGHT to $rightNeighbor")
      hasRightFork = false
      client.publishEvent("pubsub", "giveLeftFork", mapOf("target" to rightNeighbor)).subscribe()
    }
    thinking()
  }

  private fun thinking() {
    state = State.THINKING
    println("$id thinking")
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

  override fun onGiveLeftFork() {
    if (state == State.HUNGRY) {
      hasLeftFork = true
      leftForkDirty = false
      println("$id received LEFT fork, forks: $hasLeftFork $hasRightFork")
      tryEat()
    }
  }

  override fun onGiveRightFork() {
    if (state == State.HUNGRY) {
      hasRightFork = true
      rightForkDirty = false
      println("$id received RIGHT fork, forks: $hasLeftFork $hasRightFork")
      tryEat()
    }
  }

  override fun onRequestLeftFork() {
    when (state) {
      State.HUNGRY -> {
        println("$id received Left request from $leftNeighbor (in hungry)")
        hasLeftToken = true

        if (hasLeftFork && leftForkDirty) {
          println("$id surrendering dirty LEFT fork to $leftNeighbor")
          hasLeftFork = false
          client
            .publishEvent("pubsub", "giveRightFork", mapOf("target" to leftNeighbor))
            .subscribe()
          hasLeftToken = false
          client
            .publishEvent("pubsub", "requestRightFork", mapOf("target" to leftNeighbor))
            .subscribe()
        }
      }
      State.EATING -> {
        println("$id deferring LEFT fork request (eating)")
        hasLeftToken = true
      }
      State.THINKING -> {
        hasLeftToken = true
        if (hasLeftFork && leftForkDirty) {
          println("$id surrendering LEFT fork (thinking) to $leftNeighbor")
          hasLeftFork = false
          client
            .publishEvent("pubsub", "giveRightFork", mapOf("target" to leftNeighbor))
            .subscribe()
        }
      }
    }
  }

  override fun onRequestRightFork() {
    when (state) {
      State.HUNGRY -> {
        println("$id received RIGHT request from $rightNeighbor (in hungry)")
        hasRightToken = true
        if (hasRightFork && rightForkDirty) {
          println("$id surrendering dirty RIGHT fork to $rightNeighbor")
          hasRightFork = false
          client
            .publishEvent("pubsub", "giveLeftFork", mapOf("target" to rightNeighbor))
            .subscribe()
          hasRightToken = false
          client
            .publishEvent("pubsub", "requestLeftFork", mapOf("target" to rightNeighbor))
            .subscribe()
        }
      }
      State.EATING -> {
        println("$id deferring RIGHT fork request (eating)")
        hasRightToken = true
      }
      State.THINKING -> {
        hasRightToken = true
        if (hasRightFork && rightForkDirty) {
          println("$id surrendering RIGHT fork (thinking) to $rightNeighbor")
          hasRightFork = false
          client
            .publishEvent("pubsub", "giveLeftFork", mapOf("target" to rightNeighbor))
            .subscribe()
        }
      }
    }
  }

  override fun onJoin(data: Map<String, Any>) {
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
    }
  }

  fun randomAround(base: Int, delta: Int): Int {
    return (base - delta..base + delta).random(threadRng.get())
  }
}
