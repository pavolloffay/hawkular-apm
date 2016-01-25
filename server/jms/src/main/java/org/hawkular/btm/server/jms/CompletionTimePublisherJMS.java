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
package org.hawkular.btm.server.jms;

import java.util.List;

import org.hawkular.btm.api.model.events.CompletionTime;
import org.hawkular.btm.server.api.services.CompletionTimePublisher;

/**
 * This class represents the completion time JMS publisher.
 *
 * @author gbrown
 */
public class CompletionTimePublisherJMS extends AbstractPublisherJMS<CompletionTime>
implements CompletionTimePublisher {

    /**  */
    private static final int MAX_RETRIES = 3;
    private static final String DESTINATION = "java:/CompletionTimes";

    /* (non-Javadoc)
     * @see org.hawkular.btm.server.jms.AbstractPublisherJMS#getDestinationURI()
     */
    @Override
    protected String getDestinationURI() {
        return DESTINATION;
    }

    /* (non-Javadoc)
     * @see org.hawkular.btm.server.api.services.CompletionTimePublisher#publish(java.lang.String, java.util.List)
     */
    @Override
    public void publish(String tenantId, List<CompletionTime> cts) throws Exception {
        doPublish(tenantId, cts, MAX_RETRIES);
    }

}
