package ac.at.uibk.dps.dapr.chameneos.chameneos

import io.dapr.Topic
import io.dapr.actors.ActorId
import io.dapr.actors.client.ActorClient
import io.dapr.actors.client.ActorProxyBuilder
import io.dapr.client.domain.CloudEvent
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RestController

@RestController
@ConditionalOnProperty("app.role", havingValue = "chameneos")
class ChameneosSubscriber {

  private val id = System.getenv("CHAMENEOS_ID")
  private val chameneosProxy =
    ActorProxyBuilder(ChameneosActor::class.java, ActorClient()).build(ActorId("$id"))

  @Topic(name = "matchMade", pubsubName = "pubsub")
  @PostMapping("/matchMade")
  fun matchMadeSubscriber(@RequestBody event: CloudEvent<Map<String, Any>>) {
    if (event.data["target"] == id) {
      chameneosProxy.matchMade(event.data)
    }
  }

  @Topic(name = "change", pubsubName = "pubsub")
  @PostMapping("/change")
  fun changeSubscriber(@RequestBody event: CloudEvent<Map<String, Any>>) {
    if (event.data["partner"] == id) {
      chameneosProxy.change(event.data)
    }
  }
}
