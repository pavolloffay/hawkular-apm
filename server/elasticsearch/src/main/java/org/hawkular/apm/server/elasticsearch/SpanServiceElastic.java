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

package org.hawkular.apm.server.elasticsearch;

import java.util.List;

import org.hawkular.apm.api.model.span.Span;
import org.hawkular.apm.server.api.SpanStore;
import org.hawkular.apm.server.elasticsearch.log.MsgLogger;

/**
 * @author Pavol Loffay
 */
public class SpanServiceElastic implements SpanStore {

    private static final MsgLogger log = MsgLogger.LOGGER;

    private static final String SPAN_TYPE = "span";

    private ElasticsearchClient client = ElasticsearchClient.getSingleton();

    @Override
    public void store(String tenantId, List<Span> spans) {
        client.initTenant(tenantId);

        BulkRequestBuilder bulkRequestBuilder = client.getClient().prepareBulk();

        for (Span span : spans) {
            String json;
            try {
                json = serialize(span);
            } catch(IOException ex){
                log.errorf("Failed to serialize span %s", span);
                throw new StoreException(ex);
            }

            log.tracef("Storing span: %s", json);
            // modified id is used in index
            final String modifiedId = spanIdSupplier.apply(span);

            bulkRequestBuilder.add(client.getClient()
                    .prepareIndex(client.getIndex(tenantId), SPAN_TYPE, modifiedId)
                    .setSource(json));
        }

        BulkResponse bulkItemResponses = bulkRequestBuilder.execute().actionGet();

        if (bulkItemResponses.hasFailures()) {
            log.tracef("Failed to store spans to elasticsearch: %s", bulkItemResponses.buildFailureMessage());
            throw new StoreException(bulkItemResponses.buildFailureMessage());
        }

        log.trace("Success storing spans to elasticsearch");


    }

    @Override
    public void get(String tenantId, String traceId, String id) {

    }

    @Override
    public List<Span> getTrace(String tenantId, String traceId) {
        return null;
    }
}
