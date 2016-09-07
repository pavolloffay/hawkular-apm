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

import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.hawkular.apm.api.model.events.CompletionTime;
import org.hawkular.apm.api.model.events.NodeDetails;
import org.hawkular.apm.api.model.trace.CorrelationIdentifier;
import org.hawkular.apm.api.services.Criteria;
import org.hawkular.apm.api.services.Criteria.FaultCriteria;
import org.hawkular.apm.api.services.Criteria.Operator;
import org.hawkular.apm.api.services.Criteria.PropertyCriteria;

/**
 * This class provides utility functions for working with Elasticsearch.
 *
 * @author gbrown
 */
public class ElasticsearchUtil {

    /**
     * This method builds the Elasticsearch query based on the supplied
     * criteria.
     *
     * @param timeProperty The name of the time property
     * @param businessTxnProperty The name of the business transaction property
     * @param criteria the criteria
     * @param targetClass The class being queried
     * @return The query
     */
    public static BoolQueryBuilder buildQuery(Criteria criteria, String timeProperty,
            String businessTxnProperty, Class<?> targetClass) {
        long startTime = criteria.calculateStartTime();
        long endTime = criteria.calculateEndTime();

        BoolQueryBuilder query = QueryBuilders.boolQuery()
                .must(QueryBuilders.rangeQuery(timeProperty).from(startTime).to(endTime));

        if (criteria.getBusinessTransaction() != null
                && !criteria.getBusinessTransaction().trim().isEmpty()) {
            query = query.must(QueryBuilders.termQuery(businessTxnProperty, criteria.getBusinessTransaction()));
        }

        if (!criteria.getProperties().isEmpty()) {
            for (PropertyCriteria pc : criteria.getProperties()) {
                if (pc.getOperator() == Operator.HAS
                        || pc.getOperator() == Operator.HASNOT
                        || pc.getOperator() == Operator.EQ
                        || pc.getOperator() == Operator.NE) {
                    BoolQueryBuilder nestedQuery = QueryBuilders.boolQuery()
                            .must(QueryBuilders.matchQuery("properties.name", pc.getName()))
                            .must(QueryBuilders.matchQuery("properties.value", pc.getValue()));
                    if (pc.getOperator() == Operator.HASNOT
                            || pc.getOperator() == Operator.NE) {
                        query = query.mustNot(QueryBuilders.nestedQuery("properties", nestedQuery));
                    } else {
                        query = query.must(QueryBuilders.nestedQuery("properties", nestedQuery));
                    }
                } else {
                    // Numerical query
                    RangeQueryBuilder rangeQuery = null;
                    if (pc.getOperator() == Operator.GTE) {
                        rangeQuery = QueryBuilders.rangeQuery("properties.number").gte(pc.getValue());
                    } else if (pc.getOperator() == Operator.GT) {
                        rangeQuery = QueryBuilders.rangeQuery("properties.number").gt(pc.getValue());
                    } else if (pc.getOperator() == Operator.LTE) {
                        rangeQuery = QueryBuilders.rangeQuery("properties.number").lte(pc.getValue());
                    } else if (pc.getOperator() == Operator.LT) {
                        rangeQuery = QueryBuilders.rangeQuery("properties.number").lt(pc.getValue());
                    } else {
                        throw new IllegalArgumentException("Unknown property criteria operator: "+pc);
                    }
                    BoolQueryBuilder nestedQuery = QueryBuilders.boolQuery()
                            .must(QueryBuilders.matchQuery("properties.name", pc.getName()))
                            .must(rangeQuery);
                    query = query.must(QueryBuilders.nestedQuery("properties", nestedQuery));
                }
            }
        }

        if (criteria.getHostName() != null && !criteria.getHostName().trim().isEmpty()) {
            query = query.must(QueryBuilders.matchQuery("hostName", criteria.getHostName()));
        }

        if (criteria.getPrincipal() != null && !criteria.getPrincipal().trim().isEmpty()) {
            query = query.must(QueryBuilders.matchQuery("principal", criteria.getPrincipal()));
        }

        if (!criteria.getCorrelationIds().isEmpty()) {
            for (CorrelationIdentifier id : criteria.getCorrelationIds()) {
                query.must(QueryBuilders.termQuery("value", id.getValue()));
                /* HWKBTM-186
                b2 = b2.must(QueryBuilders.nestedQuery("nodes.correlationIds", // Path
                        QueryBuilders.boolQuery()
                                .must(QueryBuilders.matchQuery("correlationIds.scope", id.getScope()))
                                .must(QueryBuilders.matchQuery("correlationIds.value", id.getValue()))));
                 */
            }
        }

        if (!criteria.getFaults().isEmpty()) {
            for (FaultCriteria fc : criteria.getFaults()) {
                if (fc.getOperator() == Operator.HASNOT) {
                    query = query.mustNot(QueryBuilders.matchQuery("fault", fc.getValue()));
                } else {
                    query = query.must(QueryBuilders.matchQuery("fault", fc.getValue()));
                }
            }
        }

        if (criteria.getLowerBound() > 0
                || criteria.getUpperBound() > 0) {
            RangeQueryBuilder rangeQuery = QueryBuilders.rangeQuery("duration");
            if (criteria.getLowerBound() > 0) {
                rangeQuery.gte(criteria.getLowerBound());
            }
            if (criteria.getUpperBound() > 0) {
                rangeQuery.lte(criteria.getUpperBound());
            }
            query = query.must(rangeQuery);
        }

        // Querying uri and operation are only relevant to NodeDetails and CompletionTime
        if (targetClass == NodeDetails.class || targetClass == CompletionTime.class) {
            if (criteria.getUri() != null && !criteria.getUri().trim().isEmpty()) {
                query = query.must(QueryBuilders.matchQuery("uri", criteria.getUri()));
            }
            if (criteria.getOperation() != null && !criteria.getOperation().trim().isEmpty()) {
                query = query.must(QueryBuilders.matchQuery("operation", criteria.getOperation()));
            }
        }

        return query;
    }

    /**
     * This method returns a filter associated with the supplied criteria.
     *
     * @param criteria The criteria
     * @return The filter, or null if not relevant
     */
    public static QueryBuilder buildFilter(Criteria criteria) {
        if (criteria.getBusinessTransaction() != null && criteria.getBusinessTransaction().trim().isEmpty()) {
            return QueryBuilders.missingQuery("businessTransaction");
        }
        return null;
    }

}
