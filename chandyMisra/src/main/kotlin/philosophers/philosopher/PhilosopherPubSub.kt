package ac.at.uibk.dps.dapr.philosophers.philosopher

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
@ConditionalOnProperty("app.role", havingValue = "philosopher")
class PhilosopherPubSub {
  private val id = System.getenv("RUNTIME_ID").toInt()
  private val n = 6

  @Topic(name = "giveLeftFork", pubsubName = "pubsub")
  @PostMapping("/giveLeftFork")
  fun giveLeftForkSubscriber(@RequestBody event: CloudEvent<Map<String, Any>>) {
    if ((event.data["id"] as Int) % n == id) {
      ActorProxyBuilder(PhilosopherActor::class.java, ActorClient())
        .build(ActorId((event.data["id"] as Int).toString()))
        .onGiveLeftFork()
    }
  }

  @Topic(name = "giveRightFork", pubsubName = "pubsub")
  @PostMapping("/giveRightFork")
  fun giveRightForkSubscriber(@RequestBody event: CloudEvent<Map<String, Any>>) {
    if ((event.data["id"] as Int) % n == id) {
      ActorProxyBuilder(PhilosopherActor::class.java, ActorClient())
        .build(ActorId((event.data["id"] as Int).toString()))
        .onGiveRightFork()
    }
  }

  @Topic(name = "requestLeftFork", pubsubName = "pubsub")
  @PostMapping("/requestLeftFork")
  fun requestLeftForkSubscriber(@RequestBody event: CloudEvent<Map<String, Any>>) {
    if ((event.data["id"] as Int) % n == id) {
      ActorProxyBuilder(PhilosopherActor::class.java, ActorClient())
        .build(ActorId((event.data["id"] as Int).toString()))
        .onRequestLeftFork()
    }
  }

  @Topic(name = "requestRightFork", pubsubName = "pubsub")
  @PostMapping("/requestRightFork")
  fun requestRightForkSubscriber(@RequestBody event: CloudEvent<Map<String, Any>>) {
    if ((event.data["id"] as Int) % n == id) {
      ActorProxyBuilder(PhilosopherActor::class.java, ActorClient())
        .build(ActorId((event.data["id"] as Int).toString()))
        .onRequestRightFork()
    }
  }
}
