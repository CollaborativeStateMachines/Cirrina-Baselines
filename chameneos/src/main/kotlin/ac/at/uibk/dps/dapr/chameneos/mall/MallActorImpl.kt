package ac.at.uibk.dps.dapr.chameneos.mall

import io.dapr.actors.ActorId
import io.dapr.actors.runtime.AbstractActor
import io.dapr.actors.runtime.ActorRuntimeContext
import io.dapr.client.DaprClientBuilder

class MallActorImpl(runtimeContext: ActorRuntimeContext<MallActorImpl>, actorId: ActorId) :
    AbstractActor(runtimeContext, actorId), MallActor {

  val client = DaprClientBuilder().build()
  var count = 0
  var startTime = System.nanoTime()
  var waiting: MutableList<String> = mutableListOf()

  override fun requesting(request: MallRequest) {
    if (count == 0) startTime = System.nanoTime()
    if (waiting.isEmpty()) {
      waiting.add(request.requester)
    } else {
      count++
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
  }
}
