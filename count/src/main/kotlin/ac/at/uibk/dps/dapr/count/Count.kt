package ac.at.uibk.dps.dapr.count

import ac.at.uibk.dps.dapr.count.counter.CounterActorImpl
import ac.at.uibk.dps.dapr.count.producer.ProducerActor
import ac.at.uibk.dps.dapr.count.producer.ProducerActorImpl
import io.dapr.actors.ActorId
import io.dapr.actors.client.ActorClient
import io.dapr.actors.client.ActorProxyBuilder
import io.dapr.actors.runtime.ActorRuntime
import org.springframework.boot.ApplicationArguments
import org.springframework.boot.ApplicationRunner
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.stereotype.Component

@SpringBootApplication class Count

fun main(args: Array<String>) {
  val role = System.getenv("ROLE")
  if (role == "producer") ActorRuntime.getInstance().registerActor(ProducerActorImpl::class.java)
  if (role == "counter") ActorRuntime.getInstance().registerActor(CounterActorImpl::class.java)
  runApplication<Count>(*args)
}

@Component
class AutoStarter : ApplicationRunner {

  override fun run(args: ApplicationArguments?) {
    val role = System.getenv("ROLE") ?: "producer"
    if (role != "producer") return

    val proxy =
        ActorProxyBuilder(ProducerActor::class.java, ActorClient()).build(ActorId("producer-1"))
    proxy.produce()
  }
}
