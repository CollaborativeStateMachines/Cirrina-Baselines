package ac.at.uibk.dps.dapr.chameneos

import ac.at.uibk.dps.dapr.chameneos.chameneos.ChameneosActor
import ac.at.uibk.dps.dapr.chameneos.chameneos.ChameneosActorImpl
import ac.at.uibk.dps.dapr.chameneos.mall.MallActorImpl
import io.dapr.actors.ActorId
import io.dapr.actors.client.ActorClient
import io.dapr.actors.client.ActorProxyBuilder
import io.dapr.actors.runtime.ActorRuntime
import org.springframework.boot.ApplicationArguments
import org.springframework.boot.ApplicationRunner
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.stereotype.Component

@SpringBootApplication class Chameneos

fun main(args: Array<String>) {
  val role = System.getenv("ROLE") ?: "chameneos"
  if (role == "mall") ActorRuntime.getInstance().registerActor(MallActorImpl::class.java)
  if (role == "chameneos") ActorRuntime.getInstance().registerActor(ChameneosActorImpl::class.java)
  runApplication<Chameneos>(*args)
}

@Component
class AutoStarter : ApplicationRunner {

  override fun run(args: ApplicationArguments?) {
    val role = System.getenv("ROLE") ?: "chameneos"
    if (role != "chameneos") return

    val myId = System.getenv("CHAMENEOS_ID") ?: return

    val proxy = ActorProxyBuilder(ChameneosActor::class.java, ActorClient()).build(ActorId(myId))
    proxy.request()
  }
}
