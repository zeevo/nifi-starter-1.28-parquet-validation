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
