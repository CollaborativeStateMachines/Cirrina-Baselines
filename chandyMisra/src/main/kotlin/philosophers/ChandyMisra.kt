package ac.at.uibk.dps.dapr.philosophers

import ac.at.uibk.dps.dapr.philosophers.instantiator.InstantiatorActorImpl
import ac.at.uibk.dps.dapr.philosophers.philosopher.PhilosopherActor
import ac.at.uibk.dps.dapr.philosophers.philosopher.PhilosopherActorImpl
import com.codahale.metrics.CsvReporter
import com.codahale.metrics.MetricRegistry
import io.dapr.actors.ActorId
import io.dapr.actors.client.ActorClient
import io.dapr.actors.client.ActorProxyBuilder
import io.dapr.actors.runtime.ActorRuntime
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
import org.springframework.boot.ApplicationArguments
import org.springframework.boot.ApplicationRunner
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.stereotype.Component

@SpringBootApplication
class ChandyMisra {
  companion object {
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
  if (role == "instantiator") {
    ActorRuntime.getInstance().registerActor(InstantiatorActorImpl::class.java)
  }
  if (role == "philosopher")
    ActorRuntime.getInstance().registerActor(PhilosopherActorImpl::class.java)
  runApplication<ChandyMisra>(*args)
}

@Component
class AutoStarter : ApplicationRunner {

  override fun run(args: ApplicationArguments?) {
    val role = System.getenv("ROLE") ?: "philosopher"
    if (role == "instantiator") return
    val id = System.getenv("RUNTIME_ID")
    val proxy = ActorProxyBuilder(PhilosopherActor::class.java, ActorClient()).build(ActorId(id))
    proxy.hungry()
  }
}
