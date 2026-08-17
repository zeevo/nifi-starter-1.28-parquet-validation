/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.example.nifi.processors;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.stream.io.StreamUtils;
import org.example.nifi.api.StarterService;

@Tags({"starter", "example", "transform"})
@CapabilityDescription("Reads FlowFile content as text, transforms it with the configured StarterService, "
        + "and writes the result back to the FlowFile.")
@InputRequirement(Requirement.INPUT_REQUIRED)
@SideEffectFree
@SupportsBatching
@WritesAttributes({
        @WritesAttribute(attribute = TransformContentProcessor.SERVICE_ATTRIBUTE,
                description = "Identifier of the StarterService that transformed the content")
})
public class TransformContentProcessor extends AbstractProcessor {

    static final String SERVICE_ATTRIBUTE = "starter.service.id";

    public static final PropertyDescriptor STARTER_SERVICE = new PropertyDescriptor.Builder()
            .name("Starter Service")
            .displayName("Starter Service")
            .description("The StarterService used to transform FlowFile content")
            .identifiesControllerService(StarterService.class)
            .required(true)
            .build();

    public static final PropertyDescriptor CHARACTER_SET = new PropertyDescriptor.Builder()
            .name("Character Set")
            .displayName("Character Set")
            .description("Character set used to read and write FlowFile content")
            .required(true)
            .defaultValue("UTF-8")
            .addValidator(StandardValidators.CHARACTER_SET_VALIDATOR)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("FlowFiles whose content was transformed successfully")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("FlowFiles that could not be transformed")
            .build();

    private static final List<PropertyDescriptor> PROPERTIES =
            Collections.unmodifiableList(Arrays.asList(STARTER_SERVICE, CHARACTER_SET));

    private static final Set<Relationship> RELATIONSHIPS =
            Collections.unmodifiableSet(new HashSet<>(Arrays.asList(REL_SUCCESS, REL_FAILURE)));

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTIES;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final StarterService service = context.getProperty(STARTER_SERVICE).asControllerService(StarterService.class);
        final Charset charset = Charset.forName(context.getProperty(CHARACTER_SET).getValue());

        try {
            flowFile = session.write(flowFile, (in, out) -> {
                final String content = readContent(in, charset);
                final String transformed = service.transform(content);
                out.write(transformed.getBytes(charset));
            });
            flowFile = session.putAttribute(flowFile, SERVICE_ATTRIBUTE, service.getIdentifier());
            session.transfer(flowFile, REL_SUCCESS);
        } catch (final Exception e) {
            getLogger().error("Failed to transform {}", new Object[] {flowFile}, e);
            session.transfer(session.penalize(flowFile), REL_FAILURE);
        }
    }

    private String readContent(final InputStream in, final Charset charset) throws IOException {
        final ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        StreamUtils.copy(in, buffer);
        return new String(buffer.toByteArray(), charset);
    }

}
