package ac.at.uibk.dps.dapr.cigarette

import ac.at.uibk.dps.dapr.cigarette.arbiter.ArbiterActor
import ac.at.uibk.dps.dapr.cigarette.arbiter.ArbiterActorImpl
import ac.at.uibk.dps.dapr.cigarette.smoker.SmokerActorImpl
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
class CigaretteSmokers {
  companion object {
    val metricsDirectory = System.getenv("METRICS_DIRECTORY") ?: "metrics"

    fun provideMetricRegistry(): MetricRegistry =
      MetricRegistry().apply {
        val path = Paths.get(metricsDirectory).toAbsolutePath()
        Files.createDirectories(path)

        CsvReporter.forRegistry(this).build(path.toFile()).start(1L, TimeUnit.SECONDS)

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
  val role = System.getenv("ROLE") ?: "smoker"
  if (role == "arbiter") ActorRuntime.getInstance().registerActor(ArbiterActorImpl::class.java)
  if (role == "smoker") ActorRuntime.getInstance().registerActor(SmokerActorImpl::class.java)
  runApplication<CigaretteSmokers>(*args)
}

@Component
class AutoStarter : ApplicationRunner {

  override fun run(args: ApplicationArguments?) {
    val role = System.getenv("ROLE") ?: "smoker"
    if (role != "arbiter") return

    val proxy =
      ActorProxyBuilder(ArbiterActor::class.java, ActorClient()).build(ActorId("arbiter-1"))
    proxy.provide(0L)
  }
}
