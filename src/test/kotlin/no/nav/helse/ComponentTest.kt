package no.nav.helse

import io.ktor.config.MapApplicationConfig
import io.ktor.server.engine.connector
import io.ktor.server.testing.createTestEnvironment
import io.ktor.server.testing.withApplication
import io.ktor.util.KtorExperimentalAPI
import no.nav.common.JAASCredential
import no.nav.common.KafkaEnvironment
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test

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
    fun `should put image on topic`() {
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
                        grafanaExporterSink()
                    }
                }) {
            assertTrue(true)
        }
    }
}
