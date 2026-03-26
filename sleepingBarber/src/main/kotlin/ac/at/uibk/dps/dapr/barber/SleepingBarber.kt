package ac.at.uibk.dps.dapr.barber

import ac.at.uibk.dps.dapr.barber.barber.BarberActor
import ac.at.uibk.dps.dapr.barber.barber.BarberActorImpl
import ac.at.uibk.dps.dapr.barber.customer.CustomerActor
import ac.at.uibk.dps.dapr.barber.customer.CustomerActorImpl
import ac.at.uibk.dps.dapr.barber.waitingroom.WaitingRoomActorImpl
import com.codahale.metrics.CsvReporter
import com.codahale.metrics.MetricRegistry
import io.dapr.actors.ActorId
import io.dapr.actors.client.ActorClient
import io.dapr.actors.client.ActorProxyBuilder
import io.dapr.actors.runtime.ActorRuntime
import io.micrometer.core.instrument.binder.jvm.JvmGcMetrics
import io.micrometer.core.instrument.binder.jvm.JvmMemoryMetrics
import io.micrometer.core.instrument.binder.system.ProcessorMetrics
import io.micrometer.core.instrument.dropwizard.DropwizardConfig
import io.micrometer.core.instrument.dropwizard.DropwizardMeterRegistry
import io.micrometer.core.instrument.util.HierarchicalNameMapper
import java.nio.file.Files
import java.nio.file.Paths
import java.util.concurrent.TimeUnit
import kotlin.apply
import org.springframework.boot.ApplicationArguments
import org.springframework.boot.ApplicationRunner
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.stereotype.Component

@SpringBootApplication
class SleepingBarber {
  companion object {
    val metricsDirectory = System.getenv("METRICS_DIRECTORY") ?: "metrics"
    val metricsPeriod = System.getenv("METRICS_PERIOD")?.toLong() ?: 1L
    val metricsRegistry =
      MetricRegistry().apply {
        val path = Paths.get(metricsDirectory).toAbsolutePath()
        Files.createDirectories(path)
        CsvReporter.forRegistry(this).build(path.toFile()).start(metricsPeriod, TimeUnit.SECONDS)
        object :
            DropwizardMeterRegistry(
              object : DropwizardConfig {
                override fun get(key: String): String? = null

                override fun prefix(): String = ""
              },
              this,
              HierarchicalNameMapper.DEFAULT,
              io.micrometer.core.instrument.Clock.SYSTEM,
            ) {
            override fun nullGaugeValue(): Double = Double.NaN
          }
          .apply {
            ProcessorMetrics().bindTo(this)
            JvmMemoryMetrics().bindTo(this)
            JvmGcMetrics().bindTo(this)
          }
      }
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
    ActorClient().use { client ->
      if (role == "customer") {
        val id = System.getenv("CUSTOMER_ID")
        ActorProxyBuilder(CustomerActor::class.java, client).build(ActorId(id)).request()
      } else if (role == "barber") {
        ActorProxyBuilder(BarberActor::class.java, client).build(ActorId("barber")).sleeping()
      }
    }
  }
}
