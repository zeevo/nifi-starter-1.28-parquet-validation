package org.example.nifi.processors;

import org.apache.nifi.controller.AbstractControllerService;
import org.example.nifi.api.StarterService;

/**
 * Test double for StarterService, so this module tests against the API rather
 * than depending on nifi-starter-services.
 */
public class StubStarterService extends AbstractControllerService implements StarterService {

    private final boolean fail;

    StubStarterService() {
        this(false);
    }

    StubStarterService(final boolean fail) {
        this.fail = fail;
    }

    @Override
    public String transform(final String value) {
        if (fail) {
            throw new IllegalStateException("intentional failure");
        }
        return "stub:" + value;
    }

}
