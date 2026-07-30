package org.example.nifi.processors;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TransformContentProcessorTest {

    private static final String SERVICE_ID = "stub-starter-service";

    private TestRunner runner;

    @BeforeEach
    public void init() {
        runner = TestRunners.newTestRunner(TransformContentProcessor.class);
    }

    private void registerService(final StubStarterService service) throws InitializationException {
        runner.addControllerService(SERVICE_ID, service);
        runner.enableControllerService(service);
        runner.setProperty(TransformContentProcessor.STARTER_SERVICE, SERVICE_ID);
    }

    @Test
    public void testServiceIsRequired() {
        runner.assertNotValid();
    }

    @Test
    public void testContentTransformed() throws InitializationException {
        registerService(new StubStarterService());
        runner.assertValid();

        runner.enqueue("hello");
        runner.run();

        runner.assertAllFlowFilesTransferred(TransformContentProcessor.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(TransformContentProcessor.REL_SUCCESS).get(0);
        out.assertContentEquals("stub:hello");
        assertEquals(SERVICE_ID, out.getAttribute("starter.service.id"));
    }

    @Test
    public void testServiceFailureRoutesToFailure() throws InitializationException {
        registerService(new StubStarterService(true));

        runner.enqueue("hello");
        runner.run();

        runner.assertAllFlowFilesTransferred(TransformContentProcessor.REL_FAILURE, 1);
        runner.getFlowFilesForRelationship(TransformContentProcessor.REL_FAILURE).get(0)
                .assertContentEquals("hello");
    }

}
