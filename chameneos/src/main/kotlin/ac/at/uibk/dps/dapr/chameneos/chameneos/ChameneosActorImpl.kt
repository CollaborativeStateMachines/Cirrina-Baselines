package ac.at.uibk.dps.dapr.chameneos.chameneos

import io.dapr.actors.ActorId
import io.dapr.actors.runtime.AbstractActor
import io.dapr.actors.runtime.ActorRuntimeContext
import io.dapr.client.DaprClientBuilder
import kotlin.random.Random

class ChameneosActorImpl(
    runtimeContext: ActorRuntimeContext<ChameneosActorImpl>,
    val actorId: ActorId,
) : AbstractActor(runtimeContext, actorId), ChameneosActor {

  val client = DaprClientBuilder().build()
  var color = Random.nextInt(1, 4)

  override fun request() {
    client
        .publishEvent(
            "pubsub",
            "request",
            mapOf<String, Any>("requestor" to actorId.toString(), "color" to color),
        )
        .subscribe()
  }

  override fun meet(request: MeetRequest) {
    color = if (color == request.partnerColor) color else (color xor request.partnerColor)
    client
        .publishEvent(
            "pubsub",
            "change",
            mapOf<String, Any>("partner" to request.partner, "color" to color),
        )
        .subscribe()

    request()
  }

  override fun change(partnerColor: Int) {
    color = partnerColor
    request()
  }
}
