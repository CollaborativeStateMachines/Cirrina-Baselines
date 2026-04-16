package ac.at.uibk.dps.dapr.philosophers.philosopher

import ac.at.uibk.dps.dapr.philosophers.ChandyMisra
import io.dapr.actors.ActorId
import io.dapr.actors.runtime.AbstractActor
import io.dapr.actors.runtime.ActorRuntimeContext
import io.dapr.client.DaprClient
import io.dapr.client.DaprClientBuilder
import java.security.SecureRandom
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

  private val metricsRegistry = ChandyMisra.provideMetricRegistry()

  private val n = 6

  private var state = State.HUNGRY
  private val id = actorId.toString().toInt()
  private var hasLeftFork = id > (id - 1 + n) % n
  private var hasRightFork = id > (id + 1) % n
  private var hasLeftToken = id < (id + 1 + n) % n
  private var hasRightToken = id < (id + 1) % n
  private var leftForkDirty = id > (id - 1 + n) % n
  private var rightForkDirty = id > (id + 1) % n
  private var leftNeighbor = (id - 1 + n) % n
  private var rightNeighbor = (id + 1) % n

  private var meals: Int = 0

  override fun hungry() {
    state = State.HUNGRY
    println("$id is hungry")
    println("$id Tokens: $hasLeftToken $hasRightToken")
    println("$id Forks: $hasLeftFork $hasRightFork")
    println("$id Dirty: $leftForkDirty $rightForkDirty")

    if (!hasLeftFork && hasLeftToken) {
      println("$id requesting left fork from $leftNeighbor")
      hasLeftToken = false
      client.publishEvent("pubsub", "requestRightFork", leftNeighbor).subscribe()
    }
    if (!hasRightFork && hasRightToken) {
      println("$id requesting right fork from $rightNeighbor")
      hasRightToken = false
      client.publishEvent("pubsub", "requestLeftFork", rightNeighbor).subscribe()
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
    TODO("add timeout for ate")
  }

  private fun ate() {
    ++meals
    TODO("Add tags")

    metricsRegistry.counter("philosopher.meals").inc()

    leftForkDirty = true
    rightForkDirty = true
    println("$id done eating, deffered L= $hasLeftToken R= $hasRightToken")
    if (hasLeftToken && hasLeftFork) {
      println("$id fulfilling deferred LEFT to $leftNeighbor")
      hasLeftFork = false
      client.publishEvent("pubsub", "giveRightFork", leftNeighbor).subscribe()
    }
    if (hasRightToken && hasRightFork) {
      println("$id fulfilling deferred RIGHT to $rightNeighbor")
      hasRightFork = false
      client.publishEvent("pubsub", "giveLeftFork", rightNeighbor).subscribe()
    }
    thinking()
  }

  private fun thinking() {
    state = State.THINKING
    println("$id thinking")
  }

  override fun onGiveLeftFork() {
    if (state == State.HUNGRY) {
      hasLeftFork = true
      leftForkDirty = true
      println("$id received LEFT fork, forks: $hasLeftFork $hasRightFork")
      tryEat()
    }
  }

  override fun onGiveRightFork() {
    if (state == State.HUNGRY) {
      hasRightFork = true
      rightForkDirty = true
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
          client.publishEvent("pubsub", "giveRightFork", leftNeighbor).subscribe()
          hasLeftFork = false
          client.publishEvent("pubsub", "requestRightFork", leftNeighbor).subscribe()
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
          client.publishEvent("pubsub", "giveRightFork", leftNeighbor).subscribe()
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
          client.publishEvent("pubsub", "giveLeftFork", rightNeighbor).subscribe()
          hasRightFork = false
          client.publishEvent("pubsub", "requestLeftFork", rightNeighbor).subscribe()
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
          client.publishEvent("pubsub", "giveLeftFork", rightNeighbor).subscribe()
        }
      }
    }
  }

  fun randomAround(base: Int, delta: Int): Int {
    return (base - delta..base + delta).random(threadRng.get())
  }
}
