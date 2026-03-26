package ac.at.uibk.dps.dapr.cigarette.smoker

import io.dapr.Topic
import io.dapr.actors.ActorId
import io.dapr.actors.client.ActorClient
import io.dapr.actors.client.ActorProxyBuilder
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RestController

@RestController
@ConditionalOnProperty("app.role", havingValue = "smoker")
class SmokerSubscriber {
  private val id = System.getenv("SMOKER_ID")
  private val smokerProxy =
    ActorProxyBuilder(SmokerActor::class.java, ActorClient()).build(ActorId("$id"))

  @Topic(name = "provided", pubsubName = "pubsub")
  @PostMapping("/provided")
  fun provideSubscriber(@RequestBody body: Map<String, Any>) {
    smokerProxy.smoking(body["data"] as Map<String, Any>)
  }
}
