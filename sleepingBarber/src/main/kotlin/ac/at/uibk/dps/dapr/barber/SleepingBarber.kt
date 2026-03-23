package ac.at.uibk.dps.dapr.barber

import ac.at.uibk.dps.dapr.barber.barber.BarberActorImpl
import ac.at.uibk.dps.dapr.barber.customer.CustomerActor
import ac.at.uibk.dps.dapr.barber.customer.CustomerActorImpl
import ac.at.uibk.dps.dapr.barber.waitingroom.WaitingRoomActorImpl
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
class SleepingBarber {
  companion object {
    val logger: Logger = LoggerFactory.getLogger(SleepingBarber::class.java)
    val daprClient: DaprClient = DaprClientBuilder().build()
    val actorClient: ActorClient = ActorClient()
  }
}

fun main(args: Array<String>) {
  val role = System.getenv("ROLE") ?: "customer"
  if (role == "waiting_room")
      ActorRuntime.getInstance().registerActor(WaitingRoomActorImpl::class.java)
  if (role == "barber") ActorRuntime.getInstance().registerActor(BarberActorImpl::class.java)
  if (role == "customer") ActorRuntime.getInstance().registerActor(CustomerActorImpl::class.java)
  runApplication<SleepingBarber>(*args)
}

@Component
class AutoStarter : ApplicationRunner {

  override fun run(args: ApplicationArguments?) {
    val role = System.getenv("ROLE") ?: "customer"
    if (role != "customer") return
    val id = System.getenv("CUSTOMER_ID")
    val proxy =
        ActorProxyBuilder(CustomerActor::class.java, SleepingBarber.actorClient).build(ActorId(id))
    proxy.enterWaitingRoom().subscribe()
    SleepingBarber.logger.info("customer $id started initial")
  }
}
