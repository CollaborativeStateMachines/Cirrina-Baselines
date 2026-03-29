package ac.at.uibk.dps.dapr.barber.waitingroom

import io.dapr.Topic
import io.dapr.actors.ActorId
import io.dapr.actors.client.ActorClient
import io.dapr.actors.client.ActorProxyBuilder
import io.dapr.client.domain.CloudEvent
import kotlin.jvm.java
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RestController

@RestController
@ConditionalOnProperty("app.role", havingValue = "waiting_room")
class WaitingRoomPubSub {
  val waitingRoomActor: WaitingRoomActor? =
    ActorProxyBuilder(WaitingRoomActor::class.java, ActorClient()).build(ActorId("waitingRoom"))

  @Topic(name = "enter", pubsubName = "pubsub")
  @PostMapping("/enter")
  fun enterSubscriber(@RequestBody(required = true) event: CloudEvent<Map<String, Any>>) {
    waitingRoomActor!!.enter(event.data)
  }

  @Topic(name = "ready", pubsubName = "pubsub")
  @PostMapping("/ready")
  fun readySubscriber(@RequestBody(required = true) event: CloudEvent<Map<String, Any>>) {
    waitingRoomActor!!.ready(event.data)
  }
}
