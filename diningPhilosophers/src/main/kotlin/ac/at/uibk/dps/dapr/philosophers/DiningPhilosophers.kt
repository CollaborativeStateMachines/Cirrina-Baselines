package ac.at.uibk.dps.dapr.philosophers

import ac.at.uibk.dps.dapr.philosophers.arbitrator.ArbitratorActorImpl
import ac.at.uibk.dps.dapr.philosophers.philosopher.PhilosopherActor
import ac.at.uibk.dps.dapr.philosophers.philosopher.PhilosopherActorImpl
import io.dapr.actors.ActorId
import io.dapr.actors.client.ActorClient
import io.dapr.actors.client.ActorProxyBuilder
import io.dapr.actors.runtime.ActorRuntime
import io.dapr.client.DaprClient
import io.dapr.client.DaprClientBuilder
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.boot.ApplicationArguments
import org.springframework.boot.ApplicationRunner
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.stereotype.Component

@SpringBootApplication
class DiningPhilosophers {
  companion object {
    val logger: Logger = LoggerFactory.getLogger(DiningPhilosophers::class.java)
    val daprClient: DaprClient = DaprClientBuilder().build()
  }
}

fun main(args: Array<String>) {
  val role = System.getenv("ROLE") ?: "philosopher"
  if (role == "arbitrator")
    ActorRuntime.getInstance().registerActor(ArbitratorActorImpl::class.java)
  if (role == "philosopher")
    ActorRuntime.getInstance().registerActor(PhilosopherActorImpl::class.java)
  runApplication<DiningPhilosophers>(*args)
}

@Component
class AutoStarter : ApplicationRunner {

  override fun run(args: ApplicationArguments?) {
    val role = System.getenv("ROLE") ?: "philosopher"
    if (role == "arbitrator") return
    val id = System.getenv("PHILOSOPHER_ID")
    val proxy = ActorProxyBuilder(PhilosopherActor::class.java, ActorClient()).build(ActorId(id))
    DiningPhilosophers.logger.info("philosopher requesting initial forks")
    proxy.start().subscribe()
  }
}
