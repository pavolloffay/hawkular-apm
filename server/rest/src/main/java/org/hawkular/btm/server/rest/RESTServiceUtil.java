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
package org.hawkular.btm.server.rest;

import java.util.Set;
import java.util.StringTokenizer;

import org.hawkular.btm.api.model.trace.CorrelationIdentifier;
import org.hawkular.btm.api.model.trace.CorrelationIdentifier.Scope;
import org.hawkular.btm.api.services.Criteria.FaultCriteria;
import org.hawkular.btm.api.services.Criteria.PropertyCriteria;
import org.jboss.logging.Logger;

/**
 * Utility functions for use by REST services.
 *
 * @author gbrown
 *
 */
public class RESTServiceUtil {

    private static final Logger log = Logger.getLogger(RESTServiceUtil.class);

    /**
     * This method processes a comma separated list of properties, defined as a name|value pair.
     *
     * @param properties The properties map
     * @param encoded The string containing the encoded properties
     */
    public static void decodeProperties(Set<PropertyCriteria> properties, String encoded) {
        if (encoded != null && encoded.trim().length() > 0) {
            StringTokenizer st = new StringTokenizer(encoded, ",");
            while (st.hasMoreTokens()) {
                String token = st.nextToken();
                String[] parts = token.split("[|]");
                if (parts.length == 2) {
                    String name = parts[0].trim();
                    String value = parts[1].trim();
                    boolean excluded = false;

                    if (name.length() > 0 && name.charAt(0) == '-') {
                        name = name.substring(1);
                        excluded = true;
                    }

                    log.tracef("Extracted property name [%s] value [%s] excluded [%s]", name, value, excluded);

                    properties.add(new PropertyCriteria(name, value, excluded));
                }
            }
        }
    }

    /**
     * This method processes a comma separated list of correlation identifiers, defined as a scope|value pair.
     *
     * @param correlations The correlation identifier set
     * @param encoded The string containing the encoded correlation identifiers
     */
    public static void decodeCorrelationIdentifiers(Set<CorrelationIdentifier> correlations, String encoded) {
        if (encoded != null && encoded.trim().length() > 0) {
            StringTokenizer st = new StringTokenizer(encoded, ",");
            while (st.hasMoreTokens()) {
                String token = st.nextToken();
                String[] parts = token.split("[|]");
                if (parts.length == 2) {
                    String scope = parts[0].trim();
                    String value = parts[1].trim();

                    log.tracef("Extracted correlation identifier scope [%s] value [%s]", scope, value);

                    CorrelationIdentifier cid = new CorrelationIdentifier();
                    cid.setScope(Scope.valueOf(scope));
                    cid.setValue(value);

                    correlations.add(cid);
                }
            }
        }
    }

    /**
     * This method decodes a set of faults.
     *
     * @param faults The faults
     * @param encoded The string containing the encoded faults
     */
    public static void decodeFaults(Set<FaultCriteria> faults, String encoded) {
        if (encoded != null && encoded.trim().length() > 0) {
            StringTokenizer st = new StringTokenizer(encoded, ",");
            while (st.hasMoreTokens()) {
                String fault = st.nextToken();
                boolean excluded = false;

                if (fault.length() > 0 && fault.charAt(0) == '-') {
                    fault = fault.substring(1);
                    excluded = true;
                }

                log.tracef("Extracted fault [%s] excluded [%s]", fault, excluded);

                faults.add(new FaultCriteria(fault, excluded));
            }
        }
    }
}
