package ac.at.uibk.dps.dapr.big.big

import io.dapr.Topic
import io.dapr.actors.ActorId
import io.dapr.actors.client.ActorClient
import io.dapr.actors.client.ActorProxyBuilder
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RestController

@RestController
@ConditionalOnProperty("app.role", havingValue = "big")
class BigSubscriber {
  private val id = System.getenv("BIG_ID")

  private val bigProxy =
    ActorProxyBuilder(BigActor::class.java, ActorClient()).build(ActorId("$id"))

  @Topic(name = "neighbors", pubsubName = "pubsub")
  @PostMapping("/neighbors")
  fun handleNeighbors(@RequestBody body: Map<String, Any>) {
    val neighbors = body["data"] as List<String>

    bigProxy.assignNeighbors(neighbors)
  }

  @Topic(name = "ping", pubsubName = "pubsub")
  @PostMapping("/ping")
  fun handlePing(@RequestBody body: Map<String, Any>) {
    val data = body["data"] as? Map<String, Any> ?: body
    val receiver = data["receiver"] as? String ?: return

    if (receiver == id) {
      bigProxy.sendPong(data)
    }
  }

  @Topic(name = "pong", pubsubName = "pubsub")
  @PostMapping("/pong")
  fun handlePong(@RequestBody body: Map<String, Any>) {
    val data = body["data"] as? Map<String, Any> ?: body
    val receiver = data["receiver"] as? String ?: return

    if (receiver == id) {
      bigProxy.receivePong(data)
    }
  }
}
