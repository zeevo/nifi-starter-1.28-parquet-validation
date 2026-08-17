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
