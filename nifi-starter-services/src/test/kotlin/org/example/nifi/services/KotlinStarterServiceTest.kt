package org.example.nifi.services

import org.apache.nifi.util.TestRunner
import org.apache.nifi.util.TestRunners
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

/**
 * Kotlin test against a Kotlin service, driven through the Java ServiceTestProcessor —
 * Kotlin and Java test sources compile together in this module.
 */
class KotlinStarterServiceTest {

    private lateinit var runner: TestRunner
    private lateinit var service: KotlinStarterService

    @BeforeEach
    fun init() {
        runner = TestRunners.newTestRunner(ServiceTestProcessor::class.java)
        service = KotlinStarterService()
        runner.addControllerService(SERVICE_ID, service)
        runner.setProperty(ServiceTestProcessor.STARTER_SERVICE, SERVICE_ID)
    }

    @Test
    fun `default prefix is applied`() {
        runner.enableControllerService(service)

        runner.assertValid(service)
        assertEquals("kotlin-flow", service.transform("flow"))
    }

    @Test
    fun `custom prefix and uppercase`() {
        runner.setProperty(service, KotlinStarterService.PREFIX, "nifi::")
        runner.setProperty(service, KotlinStarterService.UPPERCASE, "true")
        runner.enableControllerService(service)

        runner.assertValid(service)
        assertEquals("NIFI::FLOW", service.transform("flow"))
    }

    @Test
    fun `empty prefix is invalid`() {
        runner.setProperty(service, KotlinStarterService.PREFIX, "")

        runner.assertNotValid(service)
    }

    private companion object {
        const val SERVICE_ID = "kotlin-starter-service"
    }
}
