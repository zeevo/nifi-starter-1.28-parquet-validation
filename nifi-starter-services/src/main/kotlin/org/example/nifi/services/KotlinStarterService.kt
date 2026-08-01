package org.example.nifi.services

import org.apache.nifi.annotation.documentation.CapabilityDescription
import org.apache.nifi.annotation.documentation.Tags
import org.apache.nifi.annotation.lifecycle.OnDisabled
import org.apache.nifi.annotation.lifecycle.OnEnabled
import org.apache.nifi.components.PropertyDescriptor
import org.apache.nifi.controller.AbstractControllerService
import org.apache.nifi.controller.ConfigurationContext
import org.apache.nifi.expression.ExpressionLanguageScope
import org.apache.nifi.processor.util.StandardValidators
import org.example.nifi.api.StarterService

/**
 * Kotlin counterpart of [StandardStarterService]. It implements the same Java interface,
 * so a Kotlin service and a Java processor (or vice versa) work together unchanged.
 *
 * Property descriptors are @JvmField in a companion object so Java code and NiFi's test
 * runner can reference them as plain static fields.
 */
@Tags("starter", "example", "kotlin")
@CapabilityDescription("Kotlin implementation of StarterService: prefixes values, optionally upper-casing them.")
class KotlinStarterService : AbstractControllerService(), StarterService {

    companion object {
        @JvmField
        val PREFIX: PropertyDescriptor = PropertyDescriptor.Builder()
            .name("Prefix")
            .displayName("Prefix")
            .description("Value prepended to every string passed to the service")
            .required(true)
            .defaultValue("kotlin-")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build()

        @JvmField
        val UPPERCASE: PropertyDescriptor = PropertyDescriptor.Builder()
            .name("Uppercase")
            .displayName("Uppercase")
            .description("Whether to upper-case the value after prefixing it")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("false")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build()

        private val PROPERTIES = listOf(PREFIX, UPPERCASE)
    }

    @Volatile
    private var prefix: String = ""

    @Volatile
    private var uppercase: Boolean = false

    override fun getSupportedPropertyDescriptors(): List<PropertyDescriptor> = PROPERTIES

    @OnEnabled
    fun onEnabled(context: ConfigurationContext) {
        prefix = context.getProperty(PREFIX).evaluateAttributeExpressions().value
        uppercase = context.getProperty(UPPERCASE).asBoolean()
        logger.debug("Enabled with prefix [{}] uppercase [{}]", prefix, uppercase)
    }

    @OnDisabled
    fun onDisabled() {
        prefix = ""
    }

    override fun transform(value: String): String =
        "$prefix$value".let { if (uppercase) it.uppercase() else it }
}
