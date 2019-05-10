package no.nav.helse

import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.model.CannedAccessControlList
import com.amazonaws.services.s3.model.PutObjectResult
import io.ktor.config.MapApplicationConfig
import io.ktor.server.engine.connector
import io.ktor.server.testing.createTestEnvironment
import io.ktor.server.testing.withApplication
import io.ktor.util.KtorExperimentalAPI
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import no.nav.common.JAASCredential
import no.nav.common.KafkaEnvironment
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.config.SaslConfigs
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.apache.kafka.common.serialization.StringSerializer
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import java.security.MessageDigest
import java.util.*
import kotlin.collections.HashMap

class ComponentTest {
    companion object {
        private const val username = "srvkafkaclient"
        private const val password = "kafkaclient"

        private val embeddedEnvironment = KafkaEnvironment(
                users = listOf(JAASCredential(username, password)),
                autoStart = false,
                withSchemaRegistry = false,
                withSecurity = true,
                topics = listOf(exportedPanelsTopic)
        )

        @BeforeAll
        @JvmStatic
        fun start() {
            embeddedEnvironment.start()
        }


        @AfterAll
        @JvmStatic
        fun stop() {
            embeddedEnvironment.tearDown()
        }
    }

    @Test
    @KtorExperimentalAPI
    fun `should read image from topic`() {
        val dashboardId = "my-dashboard"
        val panelName = "panel-name"
        val image = testImage()

        val imageMD5 = image.let {
            val md = MessageDigest.getInstance("MD5")
            md.update(it)
            Base64.getEncoder().encodeToString(md.digest())
        }

        val s3Mock = mockk<AmazonS3>(relaxed = true)

        every {
            s3Mock.putObject(match { putRequest ->
                putRequest.bucketName == exportedPanelsBucket
                        && putRequest.key == "${dashboardId}_$panelName.png"
                        && putRequest.metadata.contentLength == image.size.toLong()
                        && putRequest.metadata.contentMD5 == imageMD5
                        && putRequest.cannedAcl == CannedAccessControlList.PublicRead
            })
        } returns PutObjectResult().apply {
            contentMd5 = imageMD5
        }

        withApplication(
                environment = createTestEnvironment {
                    with (config as MapApplicationConfig) {
                        put("kafka.app-id", "component-test")
                        put("kafka.bootstrap-servers", embeddedEnvironment.brokersURL)
                        put("kafka.username", username)
                        put("kafka.password", password)
                    }

                    connector {
                        port = 8080
                    }

                    module {
                        grafanaExporterSink(s3Mock)
                    }
                }) {

            Thread.sleep(5000L)

            produceOneMessage("${dashboardId}_$panelName.png", image)
        }

        verify(exactly = 1) {
            s3Mock.putObject(match { putRequest ->
                putRequest.bucketName == exportedPanelsBucket
                        && putRequest.key == "${dashboardId}_$panelName.png"
                        && putRequest.metadata.contentLength == image.size.toLong()
                        && putRequest.metadata.contentMD5 == imageMD5
                        && putRequest.cannedAcl == CannedAccessControlList.PublicRead
            })
        }
    }

    private fun testImage() =
            ComponentTest::class.java.classLoader.getResource("test.png").readBytes()

    private fun produceOneMessage(key: String, value: ByteArray) {
        val producer = KafkaProducer<String, ByteArray>(producerProperties(), StringSerializer(), ByteArraySerializer())
        producer.send(ProducerRecord(exportedPanelsTopic, key, value)).get().let {
            println("record produced: $it")
        }
    }

    private fun producerProperties(): MutableMap<String, Any>? {
        return HashMap<String, Any>().apply {
            put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, embeddedEnvironment.brokersURL)
            put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT")
            put(SaslConfigs.SASL_MECHANISM, "PLAIN")
            put(SaslConfigs.SASL_JAAS_CONFIG, "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"$username\" password=\"$password\";")
        }
    }
}
