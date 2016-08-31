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

package org.hawkular.apm.processor.zipkin;

import java.net.URL;
import java.util.concurrent.TimeUnit;

import org.hawkular.apm.api.model.Constants;
import org.hawkular.apm.api.model.Property;
import org.hawkular.apm.api.model.events.CompletionTime;
import org.hawkular.apm.server.api.model.zipkin.Span;
import org.hawkular.apm.server.api.utils.zipkin.SpanDeriverUtil;

/**
 * @author Pavol Loffay
 */
public class CompletionTimeUtil {

    private CompletionTimeUtil() {}

    public static CompletionTime spanToCompletionTime(Span item) {
        CompletionTime completionTime = new CompletionTime();
        completionTime.setId(item.getId());

        completionTime.setTimestamp(TimeUnit.MILLISECONDS.convert(item.getTimestamp(), TimeUnit.MICROSECONDS));
        completionTime.setDuration(TimeUnit.MILLISECONDS.convert(item.getDuration(), TimeUnit.MICROSECONDS));

        completionTime.setOperation(SpanDeriverUtil.deriveOperation(item));
        completionTime.setFault(SpanDeriverUtil.deriveFault(item));

        completionTime.setHostAddress(item.ipv4());
        if (item.service() != null) {
            completionTime.getProperties().add(new Property(Constants.PROP_SERVICE_NAME, item.service()));
        }

        URL url = item.url();
        if (url != null) {
            String clientPrefix = item.clientSpan() ? Constants.URI_CLIENT_PREFIX : "";

            completionTime.setUri(clientPrefix + url.getPath());
            completionTime.setEndpointType(url.getProtocol() == null ? null : url.getProtocol().toUpperCase());
        } else {
            completionTime.setEndpointType("Unknown");
        }

        completionTime.getProperties().addAll(item.binaryAnnotationMapping().getProperties());

        return completionTime;
    }
}