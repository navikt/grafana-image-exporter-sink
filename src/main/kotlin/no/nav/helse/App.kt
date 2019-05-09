package no.nav.helse

import io.ktor.config.MapApplicationConfig
import io.ktor.server.engine.ApplicationEngineEnvironmentBuilder
import io.ktor.server.engine.applicationEngineEnvironment
import io.ktor.server.engine.connector
import io.ktor.server.engine.embeddedServer
import io.ktor.server.netty.Netty
import io.ktor.util.KtorExperimentalAPI
import java.util.concurrent.TimeUnit

@KtorExperimentalAPI
fun main() {
    val env = System.getenv()

    embeddedServer(Netty, createApplicationEnvironment(env)).let { app ->
        app.start(wait = false)

        Runtime.getRuntime().addShutdownHook(Thread {
            app.stop(1, 5, TimeUnit.SECONDS)
        })
    }
}

@KtorExperimentalAPI
fun createApplicationEnvironment(env: Map<String, String>) = applicationEngineEnvironment {
    env.configureApplicationEnvironment(this)

    connector {
        port = 8080
    }

    module {
        nais()
        grafanaExporterSink()
    }
}

@KtorExperimentalAPI
fun Map<String, String>.configureApplicationEnvironment(builder: ApplicationEngineEnvironmentBuilder) = builder.apply {
    with(config as MapApplicationConfig) {
        put("kafka.app-id", "grafana-image-exporter-sink-v1")
        put("kafka.bootstrap-servers", this@configureApplicationEnvironment.getValue("KAFKA_BOOTSTRAP_SERVERS"))
        this@configureApplicationEnvironment["KAFKA_USERNAME"]?.let { put("kafka.username", it) }
        this@configureApplicationEnvironment["KAFKA_PASSWORD"]?.let { put("kafka.password", it) }

        this@configureApplicationEnvironment["NAV_TRUSTSTORE_PATH"]?.let { put("kafka.truststore-path", it) }
        this@configureApplicationEnvironment["NAV_TRUSTSTORE_PASSWORD"]?.let { put("kafka.truststore-password", it) }
    }
}
