package ac.at.uibk.dps.dapr.chameneos.mall

import io.dapr.Topic
import io.dapr.actors.ActorId
import io.dapr.actors.client.ActorClient
import io.dapr.actors.client.ActorProxyBuilder
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RestController

@RestController
@ConditionalOnProperty("app.role", havingValue = "mall")
class MallSubscriber {
  private val actorClient = ActorClient()
  private val mallProxy =
    ActorProxyBuilder(MallActor::class.java, actorClient).build(ActorId("mall-1"))

  @Topic(name = "request", pubsubName = "pubsub")
  @PostMapping("/request")
  fun handleRequest(@RequestBody body: Map<String, Any>) {
    val data = body["data"] as? Map<*, *> ?: body
    val requestor = data["requestor"] as? String ?: return
    val color = (data["color"] as? Number)?.toInt() ?: return

    mallProxy.requesting(MallRequest(requestor, color))
  }
}
