package no.nav.helse

import io.ktor.application.Application
import io.ktor.application.ApplicationStopping
import io.ktor.application.log
import io.ktor.util.KtorExperimentalAPI
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.common.config.SaslConfigs
import org.apache.kafka.common.config.SslConfigs
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.errors.LogAndFailExceptionHandler
import java.io.File
import java.util.*

internal const val exportedPanelsTopic = "aapen-grafana-paneler-v1"

@KtorExperimentalAPI
fun Application.grafanaExporterSink() {
    val builder = StreamsBuilder()

    builder.stream<String, ByteArray>(exportedPanelsTopic)
            .map { key, value ->
                val (dashboardId, panelId) = key.split(":", limit = 2)

                KeyValue(dashboardId to panelId, value)
            }
            .foreach { (dashboardId, panelId), imageData ->
                log.info("recevied ${imageData.size} bytes for dashboard=$dashboardId and panel=$panelId")
            }

    val streams = KafkaStreams(builder.build(), streamsConfig())
    streams.start()

    environment.monitor.subscribe(ApplicationStopping) {
        streams.close()
    }
}

@KtorExperimentalAPI
private fun Application.streamsConfig() = Properties().apply {
    put(StreamsConfig.APPLICATION_ID_CONFIG, environment.config.property("kafka.app-id").getString())
    put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, environment.config.property("kafka.bootstrap-servers").getString())

    put(SaslConfigs.SASL_MECHANISM, "PLAIN")
    put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT")

    put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, LogAndFailExceptionHandler::class.java)

    environment.config.propertyOrNull("kafka.username")?.getString()?.let { username ->
        environment.config.propertyOrNull("kafka.password")?.getString()?.let { password ->
            put(SaslConfigs.SASL_JAAS_CONFIG, "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"$username\" password=\"$password\";")
        }
    }

    environment.config.propertyOrNull("kafka.truststore-path")?.getString()?.let { truststorePath ->
        environment.config.propertyOrNull("kafka.truststore-password")?.getString().let { truststorePassword ->
            try {
                put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_SSL")
                put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, File(truststorePath).absolutePath)
                put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, truststorePassword)
                log.info("Configured '${SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG}' location ")
            } catch (ex: Exception) {
                log.error("Failed to set '${SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG}' location", ex)
            }
        }
    }
}

