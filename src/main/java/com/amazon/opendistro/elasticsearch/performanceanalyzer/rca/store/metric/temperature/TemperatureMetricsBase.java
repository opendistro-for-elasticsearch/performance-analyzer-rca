/*
 * Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 *  A copy of the License is located at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  or in the "license" file accompanying this file. This file is distributed
 *  on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *  express or implied. See the License for the specific language governing
 *  permissions and limitations under the License.
 */

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.metric.temperature;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.MetricsConfiguration;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metricsdb.MetricsDB;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.temperature.TemperatureVector;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.metric.AggregateMetric;
import java.util.Arrays;
import java.util.List;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Record;
import org.jooq.Result;
import org.jooq.impl.DSL;

public abstract class TemperatureMetricsBase extends AggregateMetric {

    /**
     * Metrics are gathered each time they are sampled by the reader. Although the reader polls
     * for new writer metrics half this interval, its okay to get it every 5 seconds as this is
     * the frequency at which RCA framework runs.
     */
    public static final int METRIC_GATHER_INTERVAL = MetricsConfiguration.SAMPLING_INTERVAL / 1000;

    /**
     * This is the metricsDB aggregation column we are interested in. Pyrometer sizes for peak
     * capacity and therefore, Max is of interest here.
     */
    public static final String METRICS_DB_AGG_COLUMN_USED = MetricsDB.MAX;

    /**
     * This is the aggregate function applied to the MetricsDB aggregate column after group by.
     * Because we want to size for maximum capacity, we are interested in the max within each group.
     */
    public static final AggregateFunction AGGR_TYPE_OVER_METRICS_DB_COLUMN = AggregateFunction.SUM;

    public static final String AGGR_OVER_AGGR_NAME =
            AGGR_TYPE_OVER_METRICS_DB_COLUMN + "_of_" + METRICS_DB_AGG_COLUMN_USED;

    public TemperatureMetricsBase(TemperatureVector.Dimension metricType,
                                  String[] groupByDimensions) {
        // The temperature intendeds to spread the temperature around the cluster by re-allocating shards
        // from the hottest of nodes to the nodes that are relatively cold (with some randomness
        // so that it does not overwhelm the coldest node). Pyrometer also wants to size for peak
        // loads. Therefore, here we use MAX as the aggregate function.
        super(METRIC_GATHER_INTERVAL, metricType.NAME, AGGR_TYPE_OVER_METRICS_DB_COLUMN,
                METRICS_DB_AGG_COLUMN_USED,
                groupByDimensions);
    }

    @Override
    protected abstract Result<Record> createDslAndFetch(final DSLContext context,
                                                        final String tableName,
                                                        final Field<?> aggDimension,
                                                        final List<Field<?>> groupByFieldsList,
                                                        final List<Field<?>> selectFieldsList);

    protected List<Field<?>> aggrColumnAsSelectField() {

        final Field<Double> numDimension = DSL.field(
                DSL.name(TemperatureMetricsBase.METRICS_DB_AGG_COLUMN_USED), Double.class);
        // This aggregate function is applied after group by.
        return Arrays.asList(
                getAggDimension(numDimension, AGGR_TYPE_OVER_METRICS_DB_COLUMN).as(AGGR_OVER_AGGR_NAME));
    }
}
