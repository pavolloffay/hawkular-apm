/*
 * Copyright 2015-2016 Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.hawkular.btm.api.services;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.Map;

import org.junit.Test;

/**
 * @author gbrown
 */
public class ServiceResolverTest {

    @Test
    public void testSingletonService() {
        TestService s1 = ServiceResolver.getSingletonService(TestService.class).getNow(null);
        TestService s2 = ServiceResolver.getSingletonService(TestService.class).getNow(null);

        assertNotNull(s1);
        assertNotNull(s2);
        assertEquals("Should be singleton, so same", s1, s2);
    }

    @Test
    public void testServices() {
        List<TestService> services = ServiceResolver.getServices(TestService.class);

        assertNotNull(services);
        assertTrue("List should have two services", services.size() == 2);
    }

    @Test
    public void testNoImplementation() {
        Map<?, ?> s1 = ServiceResolver.getSingletonService(Map.class).getNow(null);

        assertNull(s1);
    }
}
