package ac.at.uibk.dps.dapr.chameneos.chameneos

import io.dapr.Topic
import io.dapr.actors.ActorId
import io.dapr.actors.client.ActorClient
import io.dapr.actors.client.ActorProxyBuilder
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RestController

@RestController
@ConditionalOnProperty("app.role", havingValue = "chameneos")
class ChameneosSubscriber {

  private val id = System.getenv("CHAMENEOS_ID")
  private val actorClient = ActorClient()
  private val chameneosProxy =
    ActorProxyBuilder(ChameneosActor::class.java, actorClient).build(ActorId("$id"))

  @Topic(name = "meet", pubsubName = "pubsub")
  @PostMapping("/meet")
  fun handleMeet(@RequestBody body: Map<String, Any>) {
    val data = body["data"] as? Map<String, Any> ?: body
    val initiator = data["initiator"] as? String ?: return

    if (initiator == id) {
      chameneosProxy.meet(data)
    }
  }

  @Topic(name = "change", pubsubName = "pubsub")
  @PostMapping("/change")
  fun handleChange(@RequestBody body: Map<String, Any>) {
    val data = body["data"] as? Map<String, Any> ?: body
    val partner = data["partner"] as? String ?: return

    if (partner == id) {
      chameneosProxy.change(data)
    }
  }
}
