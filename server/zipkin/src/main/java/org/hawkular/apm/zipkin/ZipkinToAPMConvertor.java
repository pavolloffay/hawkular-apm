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

package org.hawkular.apm.zipkin;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.hawkular.apm.api.model.trace.Consumer;
import org.hawkular.apm.api.model.trace.Node;
import org.hawkular.apm.api.model.trace.Trace;

import zipkin.Annotation;
import zipkin.BinaryAnnotation;
import zipkin.Span;

/**
 * @author Pavol Loffay
 */
public class ZipkinToAPMConvertor {

    public static List<Trace> toAPMTraces(List<Span> zipkinSpans) {
        if (zipkinSpans == null || zipkinSpans.isEmpty()) {
            return Collections.emptyList();
        }

        List<Trace> apmTraces = new ArrayList<>();
        for (Span zipkinSpan: zipkinSpans) {
            apmTraces.add(toAPMTrace(zipkinSpan));
        }

        return apmTraces;
    }

    public static Trace toAPMTrace(Span zipkinSpan) {

        Trace trace = new Trace();
        List<Node> nodes = new ArrayList<>();
        trace.setNodes(nodes);

        trace.setId(String.valueOf(zipkinSpan.id));
        trace.setStartTime(zipkinSpan.timestamp);
        trace.setBusinessTransaction(zipkinSpan.name); // todo business transaction name
        trace.setPrincipal("hawkular"); // TODO is it right?
//        trace.setHostName();
//        trace.setHostAddress();
//        trace.setNodes();
//        trace.setProperties();

//        zipkinSpan.id;
//        zipkinSpan.traceId;
//        zipkinSpan.parentId;
//        zipkinSpan.name;
//        zipkinSpan.timestamp;
//        zipkinSpan.duration;
        zipkinSpan.serviceNames();

        if (zipkinSpan.annotations != null) {
            for (Annotation zipkinAnnotation : zipkinSpan.annotations) {
//                zipkinAnnotation.timestamp;
//                zipkinAnnotation.value;

                //endpoint
//                zipkinAnnotation.endpoint.ipv4;
//                zipkinAnnotation.endpoint.port;
//                zipkinAnnotation.endpoint.serviceName;

                trace.setHostAddress(convertIpv4(zipkinAnnotation.endpoint.ipv4));


                Consumer consumer = new Consumer();
//                consumer.setCorrelationIds();
                if ("sr".equals(zipkinAnnotation.value)) {

                }
                if ("ss".equals(zipkinAnnotation.value)) {

                }
            }
        }

        if (zipkinSpan.binaryAnnotations != null) {
            for (BinaryAnnotation zipkinBinaryAnnotation: zipkinSpan.binaryAnnotations) {
//                zipkinBinaryAnnotation.value;
//                zipkinBinaryAnnotation.type;
//                zipkinBinaryAnnotation.key;
//
//                zipkinBinaryAnnotation.endpoint.ipv4;
//                zipkinBinaryAnnotation.endpoint.port;
//                zipkinBinaryAnnotation.endpoint.serviceName;
            }
        }

        return trace;
    }

    public static String convertIpv4(int ipv4) {
        return String.format("%d.%d.%d.%d",
                ipv4 >> 24 & 255,
                ipv4 >> 16 & 255,
                ipv4 >> 8 & 255,
                ipv4 & 255);
    }
}
