package ac.at.uibk.dps.dapr.philosophers

import ac.at.uibk.dps.dapr.philosophers.arbitrator.ArbitratorActorImpl
import ac.at.uibk.dps.dapr.philosophers.philosopher.PhilosopherActor
import ac.at.uibk.dps.dapr.philosophers.philosopher.PhilosopherActorImpl
import com.codahale.metrics.CsvReporter
import com.codahale.metrics.MetricRegistry
import io.dapr.actors.ActorId
import io.dapr.actors.client.ActorClient
import io.dapr.actors.client.ActorProxyBuilder
import io.dapr.actors.runtime.ActorRuntime
import io.dapr.client.DaprClient
import io.dapr.client.DaprClientBuilder
import io.micrometer.core.instrument.Clock
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
    val metricsDirectory = System.getenv("METRICS_DIRECTORY") ?: "metrics"
    val metricsPeriod = System.getenv("METRICS_PERIOD")?.toLong() ?: 1L

    fun provideMetricRegistry(): MetricRegistry =
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
              Clock.SYSTEM,
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
  val role = System.getenv("ROLE") ?: "philosopher"
  if (role == "arbitrator") {
    val numberOfPhilosophers = System.getenv("NUMBER_OF_PHILOSOPHERS").toInt()
    ActorRuntime.getInstance().registerActor(ArbitratorActorImpl::class.java) { runtime, id ->
      ArbitratorActorImpl(runtime, id, numberOfPhilosophers)
    }
  }
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
