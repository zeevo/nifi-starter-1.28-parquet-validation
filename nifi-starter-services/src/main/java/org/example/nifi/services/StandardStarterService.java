package org.example.nifi.services;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.util.StandardValidators;
import org.example.nifi.api.StarterService;

@Tags({"starter", "example"})
@CapabilityDescription("Prefixes values with a configurable string, optionally upper-casing them.")
public class StandardStarterService extends AbstractControllerService implements StarterService {

    public static final PropertyDescriptor PREFIX = new PropertyDescriptor.Builder()
            .name("Prefix")
            .displayName("Prefix")
            .description("Value prepended to every string passed to the service")
            .required(true)
            .defaultValue("starter-")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor UPPERCASE = new PropertyDescriptor.Builder()
            .name("Uppercase")
            .displayName("Uppercase")
            .description("Whether to upper-case the value after prefixing it")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("false")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();

    private static final List<PropertyDescriptor> PROPERTIES =
            Collections.unmodifiableList(Arrays.asList(PREFIX, UPPERCASE));

    private volatile String prefix;
    private volatile boolean uppercase;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTIES;
    }

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) {
        prefix = context.getProperty(PREFIX).evaluateAttributeExpressions().getValue();
        uppercase = context.getProperty(UPPERCASE).asBoolean();
        getLogger().debug("Enabled with prefix [{}] uppercase [{}]", new Object[] {prefix, uppercase});
    }

    @OnDisabled
    public void onDisabled() {
        prefix = null;
    }

    @Override
    public String transform(final String value) {
        final String transformed = prefix + value;
        return uppercase ? transformed.toUpperCase() : transformed;
    }

}
