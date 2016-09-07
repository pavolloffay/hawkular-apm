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

import static org.hawkular.apm.server.elasticsearch.ElasticsearchUtil.buildQuery;
import static org.hawkular.apm.server.elasticsearch.TraceServiceElasticsearch.TRACE_TYPE;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.elasticsearch.action.admin.indices.refresh.RefreshRequestBuilder;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.AdminClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.filter.FilterAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.filter.InternalFilter;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.bucket.missing.Missing;
import org.elasticsearch.search.aggregations.bucket.missing.MissingBuilder;
import org.elasticsearch.search.aggregations.bucket.nested.Nested;
import org.elasticsearch.search.aggregations.bucket.nested.NestedBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms.Order;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;
import org.elasticsearch.search.aggregations.metrics.avg.Avg;
import org.elasticsearch.search.aggregations.metrics.avg.AvgBuilder;
import org.elasticsearch.search.aggregations.metrics.percentiles.PercentilesBuilder;
import org.elasticsearch.search.aggregations.metrics.stats.Stats;
import org.elasticsearch.search.aggregations.metrics.stats.StatsBuilder;
import org.hawkular.apm.api.model.analytics.Cardinality;
import org.hawkular.apm.api.model.analytics.CommunicationSummaryStatistics;
import org.hawkular.apm.api.model.analytics.CommunicationSummaryStatistics.ConnectionStatistics;
import org.hawkular.apm.api.model.analytics.CompletionTimeseriesStatistics;
import org.hawkular.apm.api.model.analytics.NodeSummaryStatistics;
import org.hawkular.apm.api.model.analytics.NodeTimeseriesStatistics;
import org.hawkular.apm.api.model.analytics.Percentiles;
import org.hawkular.apm.api.model.analytics.PrincipalInfo;
import org.hawkular.apm.api.model.events.ApmEvent;
import org.hawkular.apm.api.model.events.CommunicationDetails;
import org.hawkular.apm.api.model.events.CompletionTime;
import org.hawkular.apm.api.model.events.NodeDetails;
import org.hawkular.apm.api.model.trace.Trace;
import org.hawkular.apm.api.services.AbstractAnalyticsService;
import org.hawkular.apm.api.services.Criteria;
import org.hawkular.apm.api.services.StoreException;
import org.hawkular.apm.api.utils.EndpointUtil;
import org.hawkular.apm.server.elasticsearch.log.MsgLogger;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * This class provides the Elasticsearch implementation of the Analytics
 * Service.
 *
 * @author gbrown
 */
public class AnalyticsServiceElasticsearch extends AbstractAnalyticsService {
    private static final MsgLogger msgLog = MsgLogger.LOGGER;
    private static final String COMMUNICATION_DETAILS_TYPE = "communicationdetails";
    private static final String NODE_DETAILS_TYPE = "nodedetails";
    private static final String TRACE_COMPLETION_TIME_TYPE = "tracecompletiontime";
    private static final String FRAGMENT_COMPLETION_TIME_TYPE = "fragmentcompletiontime";
    private static final ObjectMapper mapper = new ObjectMapper();
    private static ElasticsearchClient client = ElasticsearchClient.getSingleton();

    @Override
    protected List<Trace> getFragments(String tenantId, Criteria criteria) {
        return TraceServiceElasticsearch.internalQuery(client, tenantId, criteria);
    }

    @Override
    public List<PrincipalInfo> getPrincipalInfo(String tenantId, Criteria criteria) {
        String index = client.getIndex(tenantId);
        if (!refresh(index)) {
            return null;
        }

        TermsBuilder cardinalityBuilder = AggregationBuilders
                .terms("cardinality")
                .field("principal")
                .order(Order.aggregation("_count", false))
                .size(criteria.getMaxResponseSize());

        BoolQueryBuilder query = buildQuery(criteria, "startTime", "businessTransaction", Trace.class);
        SearchRequestBuilder request = getBaseSearchRequestBuilder(TRACE_TYPE, index, criteria, query, 0)
                .addAggregation(cardinalityBuilder);

        SearchResponse response = getSearchResponse(request);

        Terms terms = response.getAggregations().get("cardinality");
        return terms.getBuckets().stream()
                .map(AnalyticsServiceElasticsearch::toPrincipalInfo)
                .sorted((one, another) -> one.getId().compareTo(another.getId()))
                .collect(Collectors.toList());
    }

    @Override
    public long getTraceCompletionCount(String tenantId, Criteria criteria) {
        return getTraceCompletionCount(tenantId, criteria, false);
    }

    @Override
    public long getTraceCompletionFaultCount(String tenantId, Criteria criteria) {
        return getTraceCompletionCount(tenantId, criteria, true);
    }

    @Override
    public List<CompletionTime> getTraceCompletionTimes(String tenantId, Criteria criteria) {
        String index = client.getIndex(tenantId);
        if (!refresh(index)) {
            return null;
        }

        BoolQueryBuilder query = buildQuery(criteria, "timestamp", "businessTransaction", CompletionTime.class);
        SearchRequestBuilder request = getTraceCompletionRequest(index, criteria, query, criteria.getMaxResponseSize());
        SearchResponse response = getSearchResponse(request);
        if (response.isTimedOut()) {
            return null;
        }

        return Arrays.stream(response.getHits().getHits())
                .map(AnalyticsServiceElasticsearch::toCompletionTime)
                .filter(c -> c != null)
                .collect(Collectors.toList());
    }

    @Override
    public Percentiles getTraceCompletionPercentiles(String tenantId, Criteria criteria) {
        String index = client.getIndex(tenantId);
        if (!refresh(index)) {
            return null;
        }

        PercentilesBuilder percentileAgg = AggregationBuilders
                .percentiles("percentiles")
                .field("duration");

        BoolQueryBuilder query = buildQuery(criteria, "timestamp", "businessTransaction", CompletionTime.class);
        SearchRequestBuilder request = getTraceCompletionRequest(index, criteria, query, 0)
                .addAggregation(percentileAgg);

        SearchResponse response = getSearchResponse(request);

        org.elasticsearch.search.aggregations.metrics.percentiles.Percentiles agg =
                response.getAggregations().get("percentiles");

        Percentiles percentiles = new Percentiles();
        agg.forEach(p -> percentiles.addPercentile((int) p.getPercent(), (long) p.getValue()));
        return percentiles;
    }

    @Override
    public List<CompletionTimeseriesStatistics> getTraceCompletionTimeseriesStatistics(String tenantId, Criteria criteria, long interval) {
        String index = client.getIndex(tenantId);
        if (!refresh(index)) {
            return null;
        }

        StatsBuilder statsBuilder = AggregationBuilders
                .stats("stats")
                .field("duration");

        MissingBuilder faultCountBuilder = AggregationBuilders
                .missing("faults")
                .field("fault");

        DateHistogramBuilder histogramBuilder = AggregationBuilders
                .dateHistogram("histogram")
                .interval(interval)
                .field("timestamp")
                .subAggregation(statsBuilder)
                .subAggregation(faultCountBuilder);

        BoolQueryBuilder query = buildQuery(criteria, "timestamp", "businessTransaction", CompletionTime.class);
        SearchRequestBuilder request = getTraceCompletionRequest(index, criteria, query, 0)
                .addAggregation(histogramBuilder);

        SearchResponse response = getSearchResponse(request);
        Histogram histogram = response.getAggregations().get("histogram");

        return null;
//                .map(AnalyticsServiceElasticsearch::toCompletionTimeseriesStatistics)
//                .collect(Collectors.toList());
    }

    @Override
    public List<Cardinality> getTraceCompletionFaultDetails(String tenantId, Criteria criteria) {
        String index = client.getIndex(tenantId);
        if (!refresh(index)) {
            return null;
        }

        TermsBuilder cardinalityBuilder = AggregationBuilders
                .terms("cardinality")
                .field("fault")
                .order(Order.aggregation("_count", false))
                .size(criteria.getMaxResponseSize());

        BoolQueryBuilder query = buildQuery(criteria, "timestamp", "businessTransaction", CompletionTime.class);
        SearchRequestBuilder request = getTraceCompletionRequest(index, criteria, query, 0)
                .addAggregation(cardinalityBuilder);

        SearchResponse response = getSearchResponse(request);
        Terms terms = response.getAggregations().get("cardinality");

        return terms.getBuckets().stream()
                .map(AnalyticsServiceElasticsearch::toCardinality)
                .sorted((one, another) -> (int) (another.getCount() - one.getCount()))
                .collect(Collectors.toList());
    }

    @Override
    public List<Cardinality> getTraceCompletionPropertyDetails(String tenantId, Criteria criteria, String property) {
        String index = client.getIndex(tenantId);
        if (!refresh(index)) {
            return null;
        }

        BoolQueryBuilder nestedQuery = QueryBuilders.boolQuery()
                .must(QueryBuilders.matchQuery("properties.name", property));

        BoolQueryBuilder query = buildQuery(criteria, "timestamp", "businessTransaction", CompletionTime.class);
        query.must(QueryBuilders.nestedQuery("properties", nestedQuery));

        TermsBuilder cardinalityBuilder = AggregationBuilders
                .terms("cardinality")
                .field("properties.value")
                .order(Order.aggregation("_count", false))
                .size(criteria.getMaxResponseSize());

        FilterAggregationBuilder filterAggBuilder = AggregationBuilders
                .filter("nestedfilter")
                .filter(QueryBuilders.queryFilter(QueryBuilders.boolQuery()
                        .must(QueryBuilders.matchQuery("properties.name", property))))
                .subAggregation(cardinalityBuilder);

        NestedBuilder nestedBuilder = AggregationBuilders
                .nested("nested")
                .path("properties")
                .subAggregation(filterAggBuilder);

        SearchRequestBuilder request = getTraceCompletionRequest(index, criteria, query, 0)
                .addAggregation(nestedBuilder);

        SearchResponse response = getSearchResponse(request);
        Nested nested = response.getAggregations().get("nested");
        InternalFilter filteredAgg = nested.getAggregations().get("nestedfilter");
        Terms terms = filteredAgg.getAggregations().get("cardinality");

        return terms.getBuckets().stream()
                .map(AnalyticsServiceElasticsearch::toCardinality)
                .sorted((one, another) -> one.getValue().compareTo(another.getValue()))
                .collect(Collectors.toList());
    }

    @Override
    public List<NodeTimeseriesStatistics> getNodeTimeseriesStatistics(String tenantId, Criteria criteria, long interval) {
        String index = client.getIndex(tenantId);
        if (!refresh(index)) {
            return null;
        }

        AvgBuilder avgBuilder = AggregationBuilders
                .avg("avg")
                .field("actual");

        TermsBuilder componentsBuilder = AggregationBuilders
                .terms("components")
                .field("componentType")
                .size(criteria.getMaxResponseSize())
                .subAggregation(avgBuilder);

        DateHistogramBuilder histogramBuilder = AggregationBuilders
                .dateHistogram("histogram")
                .interval(interval)
                .field("timestamp")
                .subAggregation(componentsBuilder);

        BoolQueryBuilder query = buildQuery(criteria, "timestamp", "businessTransaction", NodeDetails.class);
        SearchRequestBuilder request = getNodeSearchRequest(index, criteria, query, 0)
                .addAggregation(histogramBuilder);

        SearchResponse response = getSearchResponse(request);
        Histogram histogram = response.getAggregations().get("histogram");

        return null;
//                .map(AnalyticsServiceElasticsearch::toNodeTimeseriesStatistics)
//                .collect(Collectors.toList());
    }

    @Override
    public Collection<NodeSummaryStatistics> getNodeSummaryStatistics(String tenantId, Criteria criteria) {
        String index = client.getIndex(tenantId);
        if (!refresh(index)) {
            return null;
        }

        List<NodeSummaryStatistics> stats = new ArrayList<>();

        AvgBuilder actualBuilder = AggregationBuilders
                .avg("actual")
                .field("actual");

        AvgBuilder elapsedBuilder = AggregationBuilders
                .avg("elapsed")
                .field("elapsed");

        TermsBuilder operationsBuilder = AggregationBuilders
                .terms("operations")
                .field("operation")
                .size(criteria.getMaxResponseSize())
                .subAggregation(actualBuilder)
                .subAggregation(elapsedBuilder);

        MissingBuilder missingOperationBuilder = AggregationBuilders
                .missing("missingOperation")
                .field("operation")
                .subAggregation(actualBuilder)
                .subAggregation(elapsedBuilder);

        TermsBuilder urisBuilder = AggregationBuilders
                .terms("uris")
                .field("uri")
                .size(criteria.getMaxResponseSize())
                .subAggregation(operationsBuilder)
                .subAggregation(missingOperationBuilder);

        TermsBuilder componentsBuilder = AggregationBuilders
                .terms("components")
                .field("componentType")
                .size(criteria.getMaxResponseSize())
                .subAggregation(urisBuilder);

        TermsBuilder interactionUrisBuilder = AggregationBuilders
                .terms("uris")
                .field("uri")
                .size(criteria.getMaxResponseSize())
                .subAggregation(actualBuilder)
                .subAggregation(elapsedBuilder);

        MissingBuilder missingComponentsBuilder = AggregationBuilders
                .missing("missingcomponent")
                .field("componentType")
                .subAggregation(interactionUrisBuilder);

        TermsBuilder nodesBuilder = AggregationBuilders
                .terms("types")
                .field("type")
                .size(criteria.getMaxResponseSize())
                .subAggregation(componentsBuilder)
                .subAggregation(missingComponentsBuilder);

        BoolQueryBuilder query = buildQuery(criteria, "timestamp", "businessTransaction", NodeDetails.class);
        SearchRequestBuilder request = getNodeSearchRequest(index, criteria, query, 0)
                .addAggregation(nodesBuilder);

        SearchResponse response = getSearchResponse(request);
        Terms types = response.getAggregations().get("types");

        for (Terms.Bucket typeBucket : types.getBuckets()) {
            Terms components = typeBucket.getAggregations().get("components");

            for (Terms.Bucket componentBucket : components.getBuckets()) {
                Terms uris = componentBucket.getAggregations().get("uris");

                for (Terms.Bucket uriBucket : uris.getBuckets()) {
                    Terms operations = uriBucket.getAggregations().get("operations");

                    for (Terms.Bucket operationBucket : operations.getBuckets()) {
                        Avg actual = operationBucket.getAggregations().get("actual");
                        Avg elapsed = operationBucket.getAggregations().get("elapsed");

                        NodeSummaryStatistics stat = new NodeSummaryStatistics();
                        stat.setComponentType(getComponentTypeForBucket(typeBucket, componentBucket));
                        stat.setUri((String)uriBucket.getKey());
                        stat.setOperation((String)operationBucket.getKey());
                        stat.setActual((long)actual.getValue());
                        stat.setElapsed((long)elapsed.getValue());
                        stat.setCount(operationBucket.getDocCount());

                        stats.add(stat);
                    }

                    Missing missingOp = uriBucket.getAggregations().get("missingOperation");
                    if (missingOp != null && missingOp.getDocCount() > 0) {
                        Avg actual = missingOp.getAggregations().get("actual");
                        Avg elapsed = missingOp.getAggregations().get("elapsed");

                        // TODO: For some reason doing comparison of value against Double.NaN does not work
                        if (!actual.getValueAsString().equals("NaN")) {
                            NodeSummaryStatistics stat = new NodeSummaryStatistics();
                            stat.setComponentType(getComponentTypeForBucket(typeBucket, componentBucket));
                            stat.setUri((String)uriBucket.getKey());
                            stat.setActual((long)actual.getValue());
                            stat.setElapsed((long)elapsed.getValue());
                            stat.setCount(missingOp.getDocCount());

                            stats.add(stat);
                        }
                    }
                }
            }

            Missing missingComponents = typeBucket.getAggregations().get("missingcomponent");

            Terms uris = missingComponents.getAggregations().get("uris");

            for (Terms.Bucket uriBucket : uris.getBuckets()) {
                Avg actual = uriBucket.getAggregations().get("actual");
                Avg elapsed = uriBucket.getAggregations().get("elapsed");

                NodeSummaryStatistics stat = new NodeSummaryStatistics();

                stat.setComponentType((String)typeBucket.getKey());
                stat.setUri((String)uriBucket.getKey());
                stat.setActual((long)actual.getValue());
                stat.setElapsed((long)elapsed.getValue());
                stat.setCount(uriBucket.getDocCount());

                stats.add(stat);
            }
        }

        return stats;
    }

    /**
     * This method returns the flat list of communication summary stats.
     *
     * @param tenantId The tenant id
     * @param criteria The criteria
     * @return The list of communication summary nodes
     */
    protected Collection<CommunicationSummaryStatistics> doGetCommunicationSummaryStatistics(String tenantId, Criteria criteria) {
        String index = client.getIndex(tenantId);
        Map<String, CommunicationSummaryStatistics> stats = new HashMap<>();

        if (!criteria.transactionWide()) {
            Criteria txnWideCriteria = criteria.deriveTransactionWide();
            buildCommunicationSummaryStatistics(stats, index, txnWideCriteria, false);
        }

        buildCommunicationSummaryStatistics(stats, index, criteria, true);
        return stats.values();
    }

    /**
     * This method builds a map of communication summary stats related to the supplied
     * criteria.
     *
     * @param stats The map of communication summary stats
     * @param index The index
     * @param criteria The criteria
     * @param addMetrics Whether to add metrics on the nodes/links
     */
    private void buildCommunicationSummaryStatistics(Map<String, CommunicationSummaryStatistics> stats, String index, Criteria criteria, boolean addMetrics) {
        if (!refresh(index)) {
            return;
        }

        // Don't specify target class, so that query provided that can be used with
        // CommunicationDetails and CompletionTime
        BoolQueryBuilder query = buildQuery(criteria, "timestamp", "businessTransaction", null);

        // Only want external communications
        query = query.mustNot(QueryBuilders.matchQuery("internal", "true"));

        StatsBuilder latencyBuilder = AggregationBuilders
                .stats("latency")
                .field("latency");

        TermsBuilder targetBuilder = AggregationBuilders
                .terms("target")
                .field("target")
                .size(criteria.getMaxResponseSize())
                .subAggregation(latencyBuilder);

        TermsBuilder sourceBuilder = AggregationBuilders
                .terms("source")
                .field("source")
                .size(criteria.getMaxResponseSize())
                .subAggregation(targetBuilder);

        SearchRequestBuilder request = getBaseSearchRequestBuilder(COMMUNICATION_DETAILS_TYPE, index, criteria, query, 0)
                .addAggregation(sourceBuilder);
        SearchResponse response = getSearchResponse(request);
        Terms sources = response.getAggregations().get("source");

        for (Terms.Bucket sourceBucket : sources.getBuckets()) {
            Terms targets = sourceBucket.getAggregations().get("target");

            String id = (String)sourceBucket.getKey();

            CommunicationSummaryStatistics css = stats.get(id);

            if (css == null) {
                css = new CommunicationSummaryStatistics();
                css.setId("TODO");
                css.setUri(EndpointUtil.decodeEndpointURI(css.getId()));
                css.setOperation(EndpointUtil.decodeEndpointOperation(css.getId(), true));
                stats.put(css.getId(), css);
            }

            if (addMetrics) {
                css.setCount(sourceBucket.getDocCount());
            }

            for (Terms.Bucket targetBucket : targets.getBuckets()) {
                Stats latency = targetBucket.getAggregations().get("latency");

                String linkId = "TODO";
                ConnectionStatistics con = css.getOutbound().get(linkId);

                if (con == null) {
                    con = new ConnectionStatistics();
                    css.getOutbound().put(linkId, con);
                }

                if (addMetrics) {
                    con.setMinimumLatency((long)latency.getMin());
                    con.setAverageLatency((long)latency.getAvg());
                    con.setMaximumLatency((long)latency.getMax());
                    con.setCount(targetBucket.getDocCount());
                }
            }
        }

        // Obtain information about the fragments
        StatsBuilder durationBuilder = AggregationBuilders
                .stats("duration")
                .field("duration");

        TermsBuilder operationsBuilder2 = AggregationBuilders
                .terms("operations")
                .field("operation")
                .size(criteria.getMaxResponseSize())
                .subAggregation(durationBuilder);

        MissingBuilder missingOperationBuilder2 = AggregationBuilders
                .missing("missingOperation")
                .field("operation")
                .subAggregation(durationBuilder);

        TermsBuilder urisBuilder2 = AggregationBuilders
                .terms("uris")
                .field("uri")
                .size(criteria.getMaxResponseSize())
                .subAggregation(operationsBuilder2)
                .subAggregation(missingOperationBuilder2);

        SearchRequestBuilder request2 = getBaseSearchRequestBuilder(FRAGMENT_COMPLETION_TIME_TYPE, index, criteria, query, 0);
        request2.addAggregation(urisBuilder2);

        SearchResponse response2 = getSearchResponse(request2);
        Terms completions = response2.getAggregations().get("uris");

        for (Terms.Bucket urisBucket : completions.getBuckets()) {
            Terms operations = urisBucket.getAggregations().get("operations");

            for (Terms.Bucket operationBucket : operations.getBuckets()) {
                Stats duration = operationBucket.getAggregations().get("duration");
//                String id = EndpointUtil.encodeEndpoint(urisBucket.getKey(),
//                        operationBucket.getKey());
                String id = "111";

                CommunicationSummaryStatistics css = stats.get(id);
                if (css == null) {
                    css = new CommunicationSummaryStatistics();
                    css.setId(id);
//                    css.setUri(urisBucket.getKey());
//                    css.setOperation(operationBucket.getKey());
                    stats.put(id, css);
                }

                if (addMetrics) {
                    doAddMetrics(css, duration, operationBucket.getDocCount());
                }
            }

            Missing missingOp = urisBucket.getAggregations().get("missingOperation");

            if (missingOp != null && missingOp.getDocCount() > 0) {
                Stats duration = missingOp.getAggregations().get("duration");
//                String id = urisBucket.getKey();

//                CommunicationSummaryStatistics css = stats.get(id);
//                if (css == null) {
//                    css = new CommunicationSummaryStatistics();
//                    css.setId(id);
//                    css.setUri(id);
//                    stats.put(id, css);
//                }

//                if (addMetrics) {
//                    doAddMetrics(css, duration, missingOp.getDocCount());
//                }
            }
        }
    }

    /* (non-Javadoc)
     * @see org.hawkular.apm.api.services.AnalyticsService#getHostNames(java.lang.String,
     *                      org.hawkular.apm.api.services.BaseCriteria)
     */
    @Override
    public Set<String> getHostNames(String tenantId, Criteria criteria) {
        String index = client.getIndex(tenantId);
        if (!refresh(index)) {
            return null;
        }

        List<Trace> btxns = TraceServiceElasticsearch.internalQuery(client, tenantId, criteria);
        return btxns.stream()
                .filter(t -> t.getHostName() != null && !t.getHostName().trim().isEmpty())
                .map(Trace::getHostName)
                .sorted()
                .collect(Collectors.toSet());
    }

    /* (non-Javadoc)
     * @see org.hawkular.apm.api.services.AnalyticsService#storeCommunicationDetails(java.lang.String, java.util.List)
     */
    @Override
    public void storeCommunicationDetails(String tenantId, List<CommunicationDetails> communicationDetails) throws StoreException {
        bulkStoreApmEvents(tenantId, communicationDetails, COMMUNICATION_DETAILS_TYPE);
    }

    /* (non-Javadoc)
     * @see org.hawkular.apm.api.services.AnalyticsService#storeNodeDetails(java.lang.String, java.util.List)
     */
    @Override
    public void storeNodeDetails(String tenantId, List<NodeDetails> nodeDetails) throws StoreException {
        bulkStoreApmEvents(tenantId, nodeDetails, NODE_DETAILS_TYPE);
    }

    /* (non-Javadoc)
     * @see org.hawkular.apm.api.services.AnalyticsService#storeCompletionTimes(java.lang.String, java.util.List)
     */
    @Override
    public void storeTraceCompletionTimes(String tenantId, List<CompletionTime> completionTimes) throws StoreException {
        bulkStoreApmEvents(tenantId, completionTimes, TRACE_COMPLETION_TIME_TYPE);
    }

    /* (non-Javadoc)
     * @see org.hawkular.apm.api.services.AnalyticsService#storeFragmentCompletionTimes(java.lang.String,
     *                                      java.util.List)
     */
    @Override
    public void storeFragmentCompletionTimes(String tenantId, List<CompletionTime> completionTimes) throws StoreException {
        bulkStoreApmEvents(tenantId, completionTimes, FRAGMENT_COMPLETION_TIME_TYPE);
    }

    private void bulkStoreApmEvents(String tenantId, List<? extends ApmEvent> events, String type) throws StoreException {
        client.initTenant(tenantId);

        BulkRequestBuilder bulkRequestBuilder = client.getClient().prepareBulk();

        for (ApmEvent event : events) {
            String json = toJson(event);
            if (null == json) {
                continue;
            }

            if (msgLog.isTraceEnabled()) {
                msgLog.tracef("Storing event: %s", json);
            }

            bulkRequestBuilder.add(toIndexRequestBuilder(client, tenantId, type, event.getId(), json));
        }

        BulkResponse bulkItemResponses = bulkRequestBuilder.execute().actionGet();

        if (bulkItemResponses.hasFailures()) {
            if (msgLog.isTraceEnabled()) {
                msgLog.trace("Failed to store event to elasticsearch: " + bulkItemResponses.buildFailureMessage());
            }
            throw new StoreException(bulkItemResponses.buildFailureMessage());
        } else {
            if (msgLog.isTraceEnabled()) {
                msgLog.trace("Success storing event to elasticsearch");
            }
        }
    }

    /* (non-Javadoc)
     * @see org.hawkular.apm.api.services.AnalyticsService#clear(java.lang.String)
     */
    @Override
    public void clear(String tenantId) {
        String index = client.getIndex(tenantId);
        try {
            client.getClient().admin().indices().prepareDelete(index).execute().actionGet();
            client.clear(tenantId);
        } catch (IndexNotFoundException ime) {
            // Ignore
        }
    }

    private static String toJson(Object ct) {
        try {
            return mapper.writeValueAsString(ct);
        } catch (JsonProcessingException e) {
            return null;
        }
    }

    private static IndexRequestBuilder toIndexRequestBuilder(ElasticsearchClient client, String tenantId, String type, String id, String json) {
        return client
                .getClient()
                .prepareIndex(client.getIndex(tenantId),type, id)
                .setSource(json);
    }

    private static NodeTimeseriesStatistics toNodeTimeseriesStatistics(Terms.Bucket bucket) {
        Terms term = bucket.getAggregations().get("components");
        NodeTimeseriesStatistics s = new NodeTimeseriesStatistics();
//        s.setTimestamp(new Date(Integer.parseInt((String)bucket.toString())).getMillis());
        term.getBuckets().forEach(b -> {
            Avg avg = b.getAggregations().get("avg");
//            NodeComponentTypeStatistics component = new NodeComponentTypeStatistics((long)avg.getValue(), b.getDocCount());
//            s.getComponentTypes().put(b.getKey(), component);
        });
        return s;
    }

    private static boolean refresh(String index) {
        try {
            AdminClient adminClient = client.getClient().admin();
            RefreshRequestBuilder refreshRequestBuilder = adminClient.indices().prepareRefresh(index);
            adminClient.indices().refresh(refreshRequestBuilder.request()).actionGet();
            return true;
        } catch (IndexNotFoundException t) {
            // Ignore, as means that no traces have
            // been stored yet
            if (msgLog.isTraceEnabled()) {
                msgLog.tracef("Index [%s] not found, unable to proceed.", index);
            }
            return false;
        }
    }

    private static Cardinality toCardinality(Terms.Bucket bucket) {
        Cardinality card = new Cardinality();
//        card.setValue(bucket.getKey());
        card.setCount(bucket.getDocCount());
        return card;
    }

    private static CompletionTimeseriesStatistics toCompletionTimeseriesStatistics(Terms.Bucket bucket) {
        Stats stat = bucket.getAggregations().get("stats");
        Missing missing = bucket.getAggregations().get("faults");

        CompletionTimeseriesStatistics s = new CompletionTimeseriesStatistics();
//        s.setTimestamp(bucket.getKeyAsDate().getMillis());
        s.setAverage((long)stat.getAvg());
        s.setMin((long)stat.getMin());
        s.setMax((long)stat.getMax());
        s.setCount(stat.getCount());
        s.setFaultCount(stat.getCount() - missing.getDocCount());
        return s;
    }

    private static CompletionTime toCompletionTime(SearchHit searchHit) {
        try {
            return mapper.readValue(searchHit.getSourceAsString(), CompletionTime.class);
        } catch (IOException e) {
            msgLog.errorFailedToParse(e);
            return null;
        }
    }

    private static PrincipalInfo toPrincipalInfo(Terms.Bucket bucket) {
        PrincipalInfo pi = new PrincipalInfo();
        pi.setId((String)bucket.getKey());
        pi.setCount(bucket.getDocCount());
        return pi;
    }

    private static SearchResponse getSearchResponse(SearchRequestBuilder request) {
        SearchResponse response = request.execute().actionGet();
        if (response.isTimedOut()) {
            msgLog.warnQueryTimedOut();
        }
        return response;
    }

    private String getComponentTypeForBucket(Terms.Bucket typeBucket, Terms.Bucket parent) {
        if (((String)typeBucket.getKey()).equalsIgnoreCase("consumer")) {
            return "consumer";
        } else if (((String)typeBucket.getKey()).equalsIgnoreCase("producer")) {
            return "producer";
        } else {
            return (String)parent.getKey();
        }
    }

    private void doAddMetrics(CommunicationSummaryStatistics css, Stats duration, long docCount) {
        css.setMinimumDuration((long)duration.getMin());
        css.setAverageDuration((long)duration.getAvg());
        css.setMaximumDuration((long)duration.getMax());
        css.setCount(docCount);
    }

    private long getTraceCompletionCount(String tenantId, Criteria criteria, boolean onlyFaulty) {
        String index = client.getIndex(tenantId);
        if (!refresh(index)) {
            return 0;
        }

        BoolQueryBuilder query = buildQuery(criteria, "timestamp", "businessTransaction", CompletionTime.class);
        SearchRequestBuilder request = getTraceCompletionRequest(index, criteria, query, 0);

        if (onlyFaulty) {
            QueryBuilder filter = QueryBuilders.existsQuery("fault");
            request.setPostFilter(filter);
        }

        SearchResponse response = request.execute().actionGet();
        if (response.isTimedOut()) {
            msgLog.warnQueryTimedOut();
            return 0;
        } else {
            return response.getHits().getTotalHits();
        }
    }

    private SearchRequestBuilder getTraceCompletionRequest(String index, Criteria criteria, BoolQueryBuilder query, int maxSize) {
        return getBaseSearchRequestBuilder(TRACE_COMPLETION_TIME_TYPE, index, criteria, query, maxSize);
    }

    private SearchRequestBuilder getNodeSearchRequest(String index, Criteria criteria, BoolQueryBuilder query, int maxSize) {
        return getBaseSearchRequestBuilder(NODE_DETAILS_TYPE, index, criteria, query, maxSize);
    }

    private SearchRequestBuilder getBaseSearchRequestBuilder(String type, String index, Criteria criteria, BoolQueryBuilder query, int maxSize) {
        return client.getClient().prepareSearch(index)
                .setTypes(type)
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setTimeout(TimeValue.timeValueMillis(criteria.getTimeout()))
                .setSize(maxSize)
                .setQuery(query);
    }

}
