package org.example.nifi.processors

import org.apache.nifi.util.TestRunner
import org.apache.nifi.util.TestRunners
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

/**
 * Kotlin test for the Kotlin processor, using the Java StubStarterService as its service
 * implementation — the processor does not care which language the service came from.
 */
class KotlinTransformProcessorTest {

    private lateinit var runner: TestRunner

    @BeforeEach
    fun init() {
        runner = TestRunners.newTestRunner(KotlinTransformProcessor::class.java)
    }

    private fun registerService(service: StubStarterService) {
        runner.addControllerService(SERVICE_ID, service)
        runner.enableControllerService(service)
        runner.setProperty(KotlinTransformProcessor.STARTER_SERVICE, SERVICE_ID)
    }

    @Test
    fun `service is required`() {
        runner.assertNotValid()
    }

    @Test
    fun `content is transformed`() {
        registerService(StubStarterService())
        runner.assertValid()

        runner.enqueue("hello")
        runner.run()

        runner.assertAllFlowFilesTransferred(KotlinTransformProcessor.REL_SUCCESS, 1)
        val out = runner.getFlowFilesForRelationship(KotlinTransformProcessor.REL_SUCCESS).first()
        out.assertContentEquals("stub:hello")
        assertEquals(SERVICE_ID, out.getAttribute(KOTLIN_SERVICE_ATTRIBUTE))
    }

    @Test
    fun `service failure routes to failure`() {
        registerService(StubStarterService(true))

        runner.enqueue("hello")
        runner.run()

        runner.assertAllFlowFilesTransferred(KotlinTransformProcessor.REL_FAILURE, 1)
        runner.getFlowFilesForRelationship(KotlinTransformProcessor.REL_FAILURE)
            .first()
            .assertContentEquals("hello")
    }

    private companion object {
        const val SERVICE_ID = "stub-starter-service"
    }
}
