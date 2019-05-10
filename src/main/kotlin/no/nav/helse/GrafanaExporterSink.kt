package no.nav.helse

import arrow.core.Try
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.model.*
import com.amazonaws.services.s3.model.lifecycle.LifecycleFilter
import io.ktor.application.Application
import io.ktor.application.ApplicationStopping
import io.ktor.application.log
import io.ktor.util.KtorExperimentalAPI
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.common.config.SaslConfigs
import org.apache.kafka.common.config.SslConfigs
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.*
import org.apache.kafka.streams.errors.LogAndFailExceptionHandler
import org.apache.kafka.streams.kstream.Consumed
import org.slf4j.MDC
import java.io.ByteArrayInputStream
import java.io.File
import java.security.MessageDigest
import java.util.*

internal const val exportedPanelsTopic = "aapen-grafana-paneler-v1"
internal const val exportedPanelsBucket = "grafana-panels"

@KtorExperimentalAPI
fun Application.grafanaExporterSink(s3Client: AmazonS3) {
    val builder = StreamsBuilder()

    ensureBucketExists(s3Client)

    builder.stream<String, ByteArray>(exportedPanelsTopic, Consumed.with(Serdes.String(), Serdes.ByteArray())
            .withOffsetResetPolicy(Topology.AutoOffsetReset.LATEST))
            .map { key, value ->
                if (key.indexOf(':') == -1) {
                    val (dashboardId, panelName) = key.split("_", limit = 2)
                    KeyValue(dashboardId to panelName, value)
                } else {
                    val (dashboardId, panelName) = key.split(":", limit = 2)
                    KeyValue(dashboardId to panelName, value)
                }
            }
            .foreach { (dashboardId, panelName), imageData ->
                try {
                    MDC.put("dashboardId", dashboardId)
                    MDC.put("panelName", panelName)

                    log.info("recevied ${imageData.size} bytes for dashboard=$dashboardId and panel=$panelName")

                    Try {
                        s3Client.putObject(PutObjectRequest(exportedPanelsBucket, "${dashboardId}_$panelName.png", ByteArrayInputStream(imageData), ObjectMetadata().apply {
                            contentLength = imageData.size.toLong()

                            val md = MessageDigest.getInstance("MD5")
                            md.update(imageData)

                            contentMD5 = Base64.getEncoder().encodeToString(md.digest())
                        }).withCannedAcl(CannedAccessControlList.PublicRead))
                    }.fold({ error ->
                        log.info("error while uploading to s3", error)
                    }, {
                        log.info("uploaded ${imageData.size} bytes for dashboard=$dashboardId and panel=$panelName")
                    })
                } finally {
                    MDC.remove("panelName")
                    MDC.remove("dashboardId")
                }
            }

    val streams = KafkaStreams(builder.build(), streamsConfig())
    streams.setUncaughtExceptionHandler { thread, err ->
        log.info("uncaught exception in thread $thread", err)
    }

    streams.start()

    environment.monitor.subscribe(ApplicationStopping) {
        streams.close()
    }
}

private fun ensureBucketExists(s3Client: AmazonS3) {
    if (s3Client.doesBucketExistV2(exportedPanelsBucket)) {
        return
    }

    s3Client.createBucket(CreateBucketRequest(exportedPanelsBucket)
            .withCannedAcl(CannedAccessControlList.PublicRead))

    s3Client.setBucketLifecycleConfiguration(exportedPanelsBucket, BucketLifecycleConfiguration()
            .withRules(BucketLifecycleConfiguration.Rule()
                    .withId("grafana-retention-policy-24h")
                    .withFilter(LifecycleFilter())
                    .withStatus(BucketLifecycleConfiguration.ENABLED)
                    .withExpirationInDays(1)))
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

