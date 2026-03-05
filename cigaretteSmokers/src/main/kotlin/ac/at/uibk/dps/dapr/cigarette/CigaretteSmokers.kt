package ac.at.uibk.dps.dapr.cigarette

import io.dapr.actors.ActorId
import io.dapr.actors.client.ActorClient
import io.dapr.actors.client.ActorProxyBuilder
import io.dapr.actors.runtime.ActorRuntime
import ac.at.uibk.dps.dapr.cigarette.arbiter.ArbiterActor
import ac.at.uibk.dps.dapr.cigarette.arbiter.ArbiterActorImpl
import ac.at.uibk.dps.dapr.cigarette.smoker.SmokerActorImpl
import org.springframework.boot.ApplicationArguments
import org.springframework.boot.ApplicationRunner
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.stereotype.Component

@SpringBootApplication
class CigaretteSmokers

fun main(args: Array<String>) {
  val role = System.getenv("ROLE") ?: "smoker"
  if (role == "arbiter") ActorRuntime.getInstance().registerActor(ArbiterActorImpl::class.java)
  if (role == "smoker") ActorRuntime.getInstance().registerActor(SmokerActorImpl::class.java)
  runApplication<CigaretteSmokers>(*args)
}

@Component
class AutoStarter : ApplicationRunner {

  override fun run(args: ApplicationArguments?) {
    val role = System.getenv("ROLE") ?: "arbiter"
    if (role != "arbiter") return

    val proxy = ActorProxyBuilder(ArbiterActor::class.java, ActorClient())
      .build(ActorId("arbiter-1"))
    proxy.provide()

    println("Arbiter provided initial ingredients")
  }
}