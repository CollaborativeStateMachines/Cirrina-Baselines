package ac.at.uibk.dps.dapr.chameneos.mall

import io.dapr.actors.ActorId
import io.dapr.actors.runtime.AbstractActor
import io.dapr.actors.runtime.ActorRuntimeContext
import io.dapr.client.DaprClientBuilder
import io.micrometer.core.instrument.Metrics

class MallActorImpl(runtimeContext: ActorRuntimeContext<MallActorImpl>, actorId: ActorId) :
  AbstractActor(runtimeContext, actorId), MallActor {

  val client = DaprClientBuilder().build()
  var count = 0
  var counter = Metrics.counter("mall.counter")
  var startTime = System.nanoTime()
  var waiting: MutableList<String> = mutableListOf()

  override fun requesting(request: MallRequest) {
    if (count == 0) startTime = System.nanoTime()
    if (count < 20000) {
      if (waiting.isEmpty()) {
        waiting.add(request.requester)
      } else {
        count++
        counter.increment()
        val waitingId = waiting.removeAt(0)
        client
          .publishEvent(
            "pubsub",
            "meet",
            mapOf<String, Any>(
              "initiator" to waitingId,
              "partner" to request.requester,
              "color" to request.color,
            ),
          )
          .subscribe()
      }
    } else if (count == 20000) {
      count++
      println("20k meetings in ${(System.nanoTime() - startTime)/ 1_000_000} ms")
    }
  }
}
