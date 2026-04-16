package ac.at.uibk.dps.dapr.philosophers.instantiator

import io.dapr.actors.ActorId
import io.dapr.actors.client.ActorClient
import io.dapr.actors.client.ActorProxyBuilder
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.web.bind.annotation.RestController

@RestController
@ConditionalOnProperty("app.role", havingValue = "instantiator")
class InstantiatorPubSub {
  val instantiatorPoxy: InstantiatorActor =
    ActorProxyBuilder(InstantiatorActor::class.java, ActorClient()).build(ActorId("instantiator"))
}
