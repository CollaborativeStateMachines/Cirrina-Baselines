package ac.at.uibk.dps.dapr.philosophers.arbitrator

import io.dapr.Topic
import io.dapr.actors.ActorId
import io.dapr.actors.client.ActorClient
import io.dapr.actors.client.ActorProxyBuilder
import io.dapr.client.DaprClient
import io.dapr.client.domain.CloudEvent
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RestController
import reactor.core.publisher.Mono

@RestController
@ConditionalOnProperty("app.role", havingValue = "arbitrator")
class ArbitratorPubSub {

  companion object {
    const val REQUEST_FORKS_TOPIC_NAME = "requestForks"
    const val DONE_EATING_TOPIC_NAME = "doneEating"
    const val PUB_SUB_NAME = "arbitrator_pub_sub"
    const val ARBITRATOR_NAME = "arbitrator"

    fun requestForks(client: DaprClient, id: Int): Mono<Void> {
      return client.publishEvent(PUB_SUB_NAME, REQUEST_FORKS_TOPIC_NAME, id)
    }

    fun doneEating(client: DaprClient, id: Int): Mono<Void> {
      return client.publishEvent(PUB_SUB_NAME, DONE_EATING_TOPIC_NAME, id)
    }
  }

  val arbitratorProxy: ArbitratorActor =
    ActorProxyBuilder(ArbitratorActor::class.java, ActorClient()).build(ActorId(ARBITRATOR_NAME))

  @Topic(name = REQUEST_FORKS_TOPIC_NAME, pubsubName = PUB_SUB_NAME)
  @PostMapping("/requestForks")
  fun requestForksSubscriber(@RequestBody(required = true) event: CloudEvent<Int>) {
    arbitratorProxy.requestForks(event.data).subscribe()
  }

  @Topic(name = DONE_EATING_TOPIC_NAME, pubsubName = PUB_SUB_NAME)
  @PostMapping("/doneEating")
  fun doneEatingSubscriber(@RequestBody(required = true) event: CloudEvent<Int>) {
    arbitratorProxy.doneEating(event.data).subscribe()
  }
}
