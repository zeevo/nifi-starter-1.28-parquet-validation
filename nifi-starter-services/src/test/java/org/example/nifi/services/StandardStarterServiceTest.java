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
