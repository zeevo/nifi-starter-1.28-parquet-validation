package org.example.nifi.processors

import java.nio.charset.Charset
import org.apache.nifi.annotation.behavior.InputRequirement
import org.apache.nifi.annotation.behavior.SideEffectFree
import org.apache.nifi.annotation.behavior.SupportsBatching
import org.apache.nifi.annotation.behavior.WritesAttribute
import org.apache.nifi.annotation.behavior.WritesAttributes
import org.apache.nifi.annotation.documentation.CapabilityDescription
import org.apache.nifi.annotation.documentation.Tags
import org.apache.nifi.components.PropertyDescriptor
import org.apache.nifi.processor.AbstractProcessor
import org.apache.nifi.processor.ProcessContext
import org.apache.nifi.processor.ProcessSession
import org.apache.nifi.processor.Relationship
import org.apache.nifi.processor.util.StandardValidators
import org.example.nifi.api.StarterService

/** Attribute written with the identifier of the service that did the transforming. */
const val KOTLIN_SERVICE_ATTRIBUTE = "starter.kotlin.service.id"

/**
 * Kotlin counterpart of [TransformContentProcessor]. Works with any StarterService,
 * whether the implementation is written in Kotlin or Java.
 */
@Tags("starter", "example", "kotlin", "transform")
@CapabilityDescription("Kotlin implementation: reads FlowFile content as text, transforms it with the configured StarterService, and writes the result back.")
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@SideEffectFree
@SupportsBatching
@WritesAttributes(
    WritesAttribute(
        attribute = KOTLIN_SERVICE_ATTRIBUTE,
        description = "Identifier of the StarterService that transformed the content"
    )
)
class KotlinTransformProcessor : AbstractProcessor() {

    companion object {
        @JvmField
        val STARTER_SERVICE: PropertyDescriptor = PropertyDescriptor.Builder()
            .name("Starter Service")
            .displayName("Starter Service")
            .description("The StarterService used to transform FlowFile content")
            .identifiesControllerService(StarterService::class.java)
            .required(true)
            .build()

        @JvmField
        val CHARACTER_SET: PropertyDescriptor = PropertyDescriptor.Builder()
            .name("Character Set")
            .displayName("Character Set")
            .description("Character set used to read and write FlowFile content")
            .required(true)
            .defaultValue("UTF-8")
            .addValidator(StandardValidators.CHARACTER_SET_VALIDATOR)
            .build()

        @JvmField
        val REL_SUCCESS: Relationship = Relationship.Builder()
            .name("success")
            .description("FlowFiles whose content was transformed successfully")
            .build()

        @JvmField
        val REL_FAILURE: Relationship = Relationship.Builder()
            .name("failure")
            .description("FlowFiles that could not be transformed")
            .build()

        private val PROPERTIES = listOf(STARTER_SERVICE, CHARACTER_SET)
        private val RELATIONSHIPS = setOf(REL_SUCCESS, REL_FAILURE)
    }

    override fun getSupportedPropertyDescriptors(): List<PropertyDescriptor> = PROPERTIES

    override fun getRelationships(): Set<Relationship> = RELATIONSHIPS

    override fun onTrigger(context: ProcessContext, session: ProcessSession) {
        var flowFile = session.get() ?: return

        val service = context.getProperty(STARTER_SERVICE).asControllerService(StarterService::class.java)
        val charset = Charset.forName(context.getProperty(CHARACTER_SET).value)

        try {
            flowFile = session.write(flowFile) { input, output ->
                val transformed = service.transform(input.reader(charset).readText())
                output.write(transformed.toByteArray(charset))
            }
            flowFile = session.putAttribute(flowFile, KOTLIN_SERVICE_ATTRIBUTE, service.identifier)
            session.transfer(flowFile, REL_SUCCESS)
        } catch (e: Exception) {
            logger.error("Failed to transform {}", arrayOf<Any>(flowFile), e)
            session.transfer(session.penalize(flowFile), REL_FAILURE)
        }
    }
}
