package ac.at.uibk.dps.dapr.philosophers.arbitrator

import ac.at.uibk.dps.dapr.philosophers.DiningPhilosophers
import ac.at.uibk.dps.dapr.philosophers.philosopher.PhilosopherPubSub
import io.dapr.actors.ActorId
import io.dapr.actors.runtime.AbstractActor
import io.dapr.actors.runtime.ActorRuntimeContext
import reactor.core.publisher.Mono

class ArbitratorActorImpl(runtimeContext: ActorRuntimeContext<ArbitratorActorImpl>, id: ActorId) :
  AbstractActor(runtimeContext, id), ArbitratorActor {

  private val numberOfPhilosophers = System.getenv("NUMBER_OF_PHILOSOPHERS").toInt()

  private val forks = BooleanArray(numberOfPhilosophers) { true }

  private val waiting = BooleanArray(numberOfPhilosophers) { false }

  private fun tryAssign(pos: Int) {
    if (forks[pos] && forks[next(pos)]) {
      forks[pos] = false
      forks[next(pos)] = false
      waiting[pos] = false
      PhilosopherPubSub.eat(DiningPhilosophers.daprClient, pos).subscribe()
    } else {
      waiting[pos] = true
    }
  }

  override fun requestForks(position: Int): Mono<Void> {
    if (position in 0 until numberOfPhilosophers) tryAssign(position)
    else DiningPhilosophers.logger.info("Invalid philosopher position: $position")
    return Mono.empty()
  }

  override fun doneEating(pos: Int): Mono<Void> {
    forks[pos] = true
    forks[next(pos)] = true
    listOf(next(pos), prev(pos), pos).filter { waiting[it] }.forEach { tryAssign(it) }
    return Mono.empty()
  }

  private fun next(i: Int) = (i + 1) % numberOfPhilosophers

  private fun prev(i: Int) = (i - 1 + numberOfPhilosophers) % numberOfPhilosophers
}
