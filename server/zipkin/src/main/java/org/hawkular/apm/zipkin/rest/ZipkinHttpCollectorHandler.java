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

package org.hawkular.apm.zipkin.rest;


import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.DataFormatException;
import java.util.zip.Inflater;

import javax.ws.rs.Consumes;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.hawkular.apm.trace.publisher.rest.client.TracePublisherRESTClient;
import org.jboss.logging.Logger;

import zipkin.Codec;
import zipkin.Span;


/**
 * Zipkin HTTP collector proxy
 *
 * @author Pavol Loffay
 */
@Path("/")
@Consumes(MediaType.WILDCARD)
@Produces(MediaType.APPLICATION_JSON)
public class ZipkinHttpCollectorHandler {

    private static final Logger log = Logger.getLogger(ZipkinHttpCollectorHandler.class);

//    private final TraceServiceRESTClient traceServiceRESTClient = new TraceServiceRESTClient();


    public ZipkinHttpCollectorHandler() {
    }

    @POST
    @Path("v1/spans")
    public Response uploadSpansJson(@HeaderParam(value = "Content-Encoding") String encoding, byte[] body) {
        return storeSpans(body, encoding, Codec.JSON);
    }

    @POST
    @Path("v1/spans")
    @Consumes("application/x-thrift")
    public Response uploadSpansThrift(@HeaderParam(value = "Content-Encoding") String encoding, byte[] body) {
        return storeSpans(body, encoding, Codec.THRIFT);
    }


    private Response storeSpans(byte[] body, String encoding, Codec codec) {

        // decode if necessary
        if (encoding != null && "gzip".equals(encoding)) {
            try {
                body = gunzip(body);
            } catch (IOException ex) {
                log.errorf("Could not decode body from %s", encoding);
                Map<String, String> errors = new HashMap<>();
                errors.put("errorMsg", "Internal Error: " + ex.getMessage());
                return Response.status(Response.Status.BAD_REQUEST).build();
            }
        }

        // store spans aka trace fragments
        try {
            List<Span> zipkinSpans = codec.readSpans(body);
            zipkinSpans = null;
            log.infof("Received zipkin spans %s", zipkinSpans);

            TracePublisherRESTClient tracePublisherRESTClient = new TracePublisherRESTClient();
            tracePublisherRESTClient.publish("hawkular", null);
        } catch (Exception e) {
            log.errorf("Could not store zipkin spans");
            Map<String, String> errors = new HashMap<>();
            errors.put("errorMsg", "Internal Error: " + e.getMessage());
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(errors).build();
        }

        return Response.ok().build();
    }

    private static final ThreadLocal<byte[]> GZIP_BUFFER = new ThreadLocal<byte[]>() {
        @Override protected byte[] initialValue() {
            return new byte[1024];
        }
    };

    static byte[] gunzip(byte[] input) throws IOException {
        Inflater inflater = new Inflater();
        inflater.setInput(input);
        try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream(input.length)) {
            while (!inflater.finished()) {
                int count = inflater.inflate(GZIP_BUFFER.get());
                outputStream.write(GZIP_BUFFER.get(), 0, count);
            }
            return outputStream.toByteArray();
        } catch (DataFormatException e) {
            throw new IOException(e.getMessage(), e);
        }
    }

}
