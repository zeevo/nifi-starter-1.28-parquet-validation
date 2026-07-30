package org.example.nifi.services;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class StandardStarterServiceTest {

    private static final String SERVICE_ID = "starter-service";

    private TestRunner runner;
    private StandardStarterService service;

    @BeforeEach
    public void init() throws InitializationException {
        runner = TestRunners.newTestRunner(ServiceTestProcessor.class);
        service = new StandardStarterService();
        runner.addControllerService(SERVICE_ID, service);
        runner.setProperty(ServiceTestProcessor.STARTER_SERVICE, SERVICE_ID);
    }

    @Test
    public void testDefaultPrefixApplied() {
        runner.enableControllerService(service);

        runner.assertValid(service);
        assertEquals("starter-flow", service.transform("flow"));
    }

    @Test
    public void testCustomPrefixAndUppercase() {
        runner.setProperty(service, StandardStarterService.PREFIX, "nifi::");
        runner.setProperty(service, StandardStarterService.UPPERCASE, "true");
        runner.enableControllerService(service);

        runner.assertValid(service);
        assertEquals("NIFI::FLOW", service.transform("flow"));
    }

    @Test
    public void testEmptyPrefixIsInvalid() {
        runner.setProperty(service, StandardStarterService.PREFIX, "");

        runner.assertNotValid(service);
    }

}
