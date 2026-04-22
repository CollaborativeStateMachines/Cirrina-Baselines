package ac.at.uibk.dps.dapr.philosophers

import ac.at.uibk.dps.dapr.philosophers.instantiator.InstantiatorActor
import ac.at.uibk.dps.dapr.philosophers.philosopher.PhilosopherActor
import io.dapr.Topic
import io.dapr.actors.ActorId
import io.dapr.actors.client.ActorClient
import io.dapr.actors.client.ActorProxyBuilder
import io.dapr.client.domain.CloudEvent
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RestController

@RestController
class DynamicPhilosophersPubSub {
  private val id = System.getenv("RUNTIME_ID").toInt()
  private val n = 12
  private val actorClient = ActorClient()
  private val philosopherProxyBuilder = ActorProxyBuilder(PhilosopherActor::class.java, actorClient)
  val instantiatorProxy: InstantiatorActor =
    ActorProxyBuilder(InstantiatorActor::class.java, actorClient).build(ActorId("instantiator$id"))

  @Topic(name = "giveLeftFork", pubsubName = "pubsub")
  @PostMapping("/giveLeftFork")
  fun giveLeftForkSubscriber(@RequestBody event: CloudEvent<Map<String, Any>>) {
    val actorNumber = event.data["target"].toString().filter { it.isDigit() }.toInt()
    if ((actorNumber % n) == id) {
      try {
        philosopherProxyBuilder
          .build(ActorId((event.data["target"].toString())))
          .onGiveLeftFork(event.data)
      } catch (e: Exception) {
        println("ERROR: [PubSub] giveLeftFork to ${event.data["target"]} failed: ${e.message}")
        throw e
      }
    }
  }

  @Topic(name = "giveRightFork", pubsubName = "pubsub")
  @PostMapping("/giveRightFork")
  fun giveRightForkSubscriber(@RequestBody event: CloudEvent<Map<String, Any>>) {
    val actorNumber = event.data["target"].toString().filter { it.isDigit() }.toInt()
    if ((actorNumber % n) == id) {
      try {
        philosopherProxyBuilder
          .build(ActorId((event.data["target"].toString())))
          .onGiveRightFork(event.data)
      } catch (e: Exception) {
        println("ERROR: [PubSub] giveRightFork to ${event.data["target"]} failed: ${e.message}")
        throw e
      }
    }
  }

  @Topic(name = "requestLeftFork", pubsubName = "pubsub")
  @PostMapping("/requestLeftFork")
  fun requestLeftForkSubscriber(@RequestBody event: CloudEvent<Map<String, Any>>) {
    val actorNumber = event.data["target"].toString().filter { it.isDigit() }.toInt()

    if ((actorNumber % n) == id) {
      try {
        philosopherProxyBuilder
          .build(ActorId((event.data["target"].toString())))
          .onRequestLeftFork(event.data)
      } catch (e: Exception) {
        println("ERROR: [PubSub] requestLeftFork to ${event.data["target"]} failed: ${e.message}")
        throw e
      }
    }
  }

  @Topic(name = "requestRightFork", pubsubName = "pubsub")
  @PostMapping("/requestRightFork")
  fun requestRightForkSubscriber(@RequestBody event: CloudEvent<Map<String, Any>>) {
    val actorNumber = event.data["target"].toString().filter { it.isDigit() }.toInt()

    if ((actorNumber % n) == id) {
      try {
        philosopherProxyBuilder
          .build(ActorId((event.data["target"].toString())))
          .onRequestRightFork(event.data)
      } catch (e: Exception) {
        println("ERROR: [PubSub] requestRightFork to ${event.data["target"]} failed: ${e.message}")
        throw e
      }
    }
  }

  @Topic(name = "join", pubsubName = "pubsub")
  @PostMapping("/join")
  fun joinSubscriber(@RequestBody event: CloudEvent<Map<String, Any>>) {
    val actorNumber = event.data["target"].toString().filter { it.isDigit() }.toInt()
    if ((actorNumber % n) == id) {
      try {
        philosopherProxyBuilder.build(ActorId((event.data["target"].toString()))).onJoin(event.data)
      } catch (e: Exception) {
        println("ERROR: [PubSub] join to ${event.data["target"]} failed: ${e.message}")
        throw e
      }
    }
  }

  @Topic(name = "instantiate", pubsubName = "pubsub")
  @PostMapping("/instantiate")
  fun instantiateSubscriber(@RequestBody event: CloudEvent<Map<String, Any>>) {
    val actorNumber = event.data["id"].toString().filter { it.isDigit() }.toInt()
    if ((actorNumber % n) == id) {
      try {
        val instance = philosopherProxyBuilder.build(ActorId((event.data["id"].toString())))
        instance.onInstantiate(event.data)
      } catch (e: Exception) {
        println("ERROR: [PubSub] instantiate ${event.data["id"]} failed: ${e.message}")
        throw e
      }
    }
  }

  @Topic(name = "nodeCreated", pubsubName = "pubsub")
  @PostMapping("/nodeCreated")
  fun nodeCreatedSubscriber(@RequestBody event: CloudEvent<Map<String, Any>>) {
    try {
      instantiatorProxy.onNodeCreated(event.data)
    } catch (e: Exception) {
      println("ERROR: [PubSub] nodeCreated failed: ${e.message}")
      throw e
    }
  }
}