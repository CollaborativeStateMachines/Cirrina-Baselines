package ac.at.uibk.dps.dapr.pingPong

import ac.at.uibk.dps.dapr.pingPong.ping.PingActor
import ac.at.uibk.dps.dapr.pingPong.ping.PingActorImpl
import ac.at.uibk.dps.dapr.pingPong.pong.PongActorImpl
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
import java.nio.file.Paths
import java.util.concurrent.TimeUnit
import org.springframework.boot.ApplicationArguments
import org.springframework.boot.ApplicationRunner
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.stereotype.Component

@SpringBootApplication
class PingPong {
  companion object {
    val metricsDirectory = System.getenv("METRICS_DIRECTORY") ?: "metrics"

    fun provideMetricRegistry(): MetricRegistry =
      MetricRegistry().apply {
        val path = Paths.get(metricsDirectory).toAbsolutePath()

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
  val role = System.getenv("ROLE")
  if (role == "ping") ActorRuntime.getInstance().registerActor(PingActorImpl::class.java)
  if (role == "pong") ActorRuntime.getInstance().registerActor(PongActorImpl::class.java)
  runApplication<PingPong>(*args)
}

@Component
class AutoStarter : ApplicationRunner {

  override fun run(args: ApplicationArguments?) {
    val role = System.getenv("ROLE") ?: "ping"
    if (role != "ping") return
    val proxy = ActorProxyBuilder(PingActor::class.java, ActorClient()).build(ActorId("ping-1"))
    proxy.ping(-1L)
  }
}
