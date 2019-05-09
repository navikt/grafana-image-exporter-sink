package no.nav.helse

import com.amazonaws.auth.AWSStaticCredentialsProvider
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.client.builder.AwsClientBuilder
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import io.ktor.config.ApplicationConfig
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
        grafanaExporterSink(s3Client(environment.config))
    }
}

@KtorExperimentalAPI
private fun s3Client(config: ApplicationConfig) =
        with (config) {
            AmazonS3ClientBuilder.standard()
                    .withEndpointConfiguration(AwsClientBuilder
                            .EndpointConfiguration(property("s3.url").getString(), "us-east-1"))
                    .enablePathStyleAccess()
                    .withCredentials(AWSStaticCredentialsProvider(BasicAWSCredentials(
                            property("s3.access-key").getString(),
                            property("s3.secret-key").getString()))).build()
        }

@KtorExperimentalAPI
fun Map<String, String>.configureApplicationEnvironment(builder: ApplicationEngineEnvironmentBuilder) = builder.apply {
    with(config as MapApplicationConfig) {
        put("s3.url", this@configureApplicationEnvironment.getOrDefault("S3_URL", "http://s3.nais-rook.svc.nais.local"))
        put("s3.access-key", this@configureApplicationEnvironment.getValue("S3_ACCESS_KEY"))
        put("s3.secret-key", this@configureApplicationEnvironment.getValue("S3_SECRET_KEY"))

        put("kafka.app-id", "grafana-image-exporter-sink-v1")
        put("kafka.bootstrap-servers", this@configureApplicationEnvironment.getValue("KAFKA_BOOTSTRAP_SERVERS"))
        this@configureApplicationEnvironment["KAFKA_USERNAME"]?.let { put("kafka.username", it) }
        this@configureApplicationEnvironment["KAFKA_PASSWORD"]?.let { put("kafka.password", it) }

        this@configureApplicationEnvironment["NAV_TRUSTSTORE_PATH"]?.let { put("kafka.truststore-path", it) }
        this@configureApplicationEnvironment["NAV_TRUSTSTORE_PASSWORD"]?.let { put("kafka.truststore-password", it) }
    }
}
