package ac.at.uibk.dps.dapr.big

import io.dapr.actors.ActorId
import io.dapr.actors.client.ActorClient
import io.dapr.actors.client.ActorProxyBuilder
import io.dapr.actors.runtime.ActorRuntime
import ac.at.uibk.dps.dapr.big.big.BigActor
import ac.at.uibk.dps.dapr.big.big.BigActorImpl
import ac.at.uibk.dps.dapr.big.sink.SinkActorImpl
import org.springframework.boot.ApplicationArguments
import org.springframework.boot.ApplicationRunner
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.stereotype.Component

@SpringBootApplication
class Big

fun main(args: Array<String>) {
  val role = System.getenv("ROLE") ?: "big"
  if (role == "sink") ActorRuntime.getInstance().registerActor(SinkActorImpl::class.java)
  if (role == "big") ActorRuntime.getInstance().registerActor(BigActorImpl::class.java)
  runApplication<Big>(*args)
}

@Component
class AutoStarter : ApplicationRunner {

  override fun run(args: ApplicationArguments?) {
    val role = System.getenv("ROLE") ?: "big"
    if (role != "big") return

    val myId = System.getenv("BIG_ID") ?: return

    val proxy = ActorProxyBuilder(BigActor::class.java, ActorClient())
      .build(ActorId(myId))
    proxy.register()

    println("[$myId] Auto-started, sent initial request to sink")
  }
}