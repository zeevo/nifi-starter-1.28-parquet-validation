package org.example.nifi.services;

import java.util.Collections;
import java.util.List;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.example.nifi.api.StarterService;

/**
 * Minimal processor used only to host the controller service under test, since
 * a TestRunner is always built around a processor.
 */
public class ServiceTestProcessor extends AbstractProcessor {

    static final PropertyDescriptor STARTER_SERVICE = new PropertyDescriptor.Builder()
            .name("Starter Service")
            .identifiesControllerService(StarterService.class)
            .required(true)
            .build();

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return Collections.singletonList(STARTER_SERVICE);
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
    }

}
