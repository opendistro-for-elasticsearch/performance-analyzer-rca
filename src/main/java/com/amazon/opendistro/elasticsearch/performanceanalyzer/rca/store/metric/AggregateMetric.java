/*
 * Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 *  permissions and limitations under the License.
 */

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.metric;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.metricsdb.MetricsDB;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Metric;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.flow_units.MetricFlowUnit;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.Queryable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Record;
import org.jooq.Result;
import org.jooq.impl.DSL;


/**
 * AggregateMetric can be used to group the sqlite to one or more columns
 * and perform sum aggregation and sorting function on the result
 * For example, we can get the sum of cpu usage for each operation and sort them in descending
 * order by constructing this Metric as follows:
 * <p>
 * new AggregateMetric(5, CPU_Utilization.NAME, CommonDimension.OPERATION.toString());
 * </p>
 */
public class AggregateMetric extends Metric {

    private static final Logger LOG = LogManager.getLogger(AggregateMetric.class);
    private final String tableName;
    private final List<String> groupByFieldsName;
    private final AggregateFunction aggregateFunction;
    private final String metricsDBAggrColumn;

    public AggregateMetric(final long evaluationIntervalSeconds, final String tableName,
                           final AggregateFunction aggregateFunction,
                           String metricsDBCol, final String... groupByFieldsName) {
        super("", evaluationIntervalSeconds);
        this.tableName = tableName;
        this.groupByFieldsName = new ArrayList<>(Arrays.asList(groupByFieldsName));
        this.aggregateFunction = aggregateFunction;

        switch (metricsDBCol) {
            case MetricsDB.SUM:
            case MetricsDB.AVG:
            case MetricsDB.MIN:
            case MetricsDB.MAX:
                this.metricsDBAggrColumn = metricsDBCol;
                break;
            default:
                throw new IllegalArgumentException("Unrecognized metricsDB col: " + metricsDBCol);
        }
    }

    protected Result<Record> createDslAndFetch(final DSLContext context,
                                               final String tableName,
                                               final Field<?> aggDimension,
                                               final List<Field<?>> groupByFieldsList,
                                               final List<Field<?>> selectFieldsList) {
        return context
                .select(selectFieldsList)
                .from(tableName)
                .groupBy(groupByFieldsList)
                .orderBy(aggDimension.desc())
                .fetch();
    }

    protected List<Field<?>> getGroupByFieldsList() {
        if (groupByFieldsName.isEmpty()) {
            return Collections.emptyList();
        }
        final List<Field<?>> groupByFieldsList = new ArrayList<>();
        groupByFieldsName.forEach(f -> groupByFieldsList.add(DSL.field(DSL.name(f))));
        return groupByFieldsList;
    }

    protected List<Field<?>> getSelectFieldsList(final List<Field<?>> groupByFields,
                                              Field<?> aggrDimension) {
        List<Field<?>> fieldsList = new ArrayList<>(groupByFields);
        fieldsList.add(aggrDimension);
        return fieldsList;
    }

    protected Field<?> getAggrDimension() {
        // This is the column from metricsDB that we want to SELECT from.
        final Field<Double> numDimension = DSL.field(DSL.name(metricsDBAggrColumn), Double.class);

        // This aggregate function is applied after group by.
        return getAggDimension(numDimension, this.aggregateFunction);
    }

    @Override
    public MetricFlowUnit gather(final Queryable queryable) {
        LOG.debug("Metric: Trying to gather metrics for {}", tableName);
        final Result<Record> result;
        List<Field<?>> selectFieldsList;
        try {
            final MetricsDB db = queryable.getMetricsDB();
            final DSLContext context = db.getDSLContext();

            final Field<?> aggDimension = getAggrDimension();
            final List<Field<?>> groupByFieldsList = getGroupByFieldsList();
            selectFieldsList = getSelectFieldsList(groupByFieldsList, aggDimension);

            result = createDslAndFetch(context, tableName, aggDimension, groupByFieldsList,
                    selectFieldsList);
        } catch (Exception e) {
            //TODO: Emit log/stats that gathering failed.
            LOG.error("RCA: Caught an exception while getting the DB {}", e.getMessage());
            return MetricFlowUnit.generic();
        }
        return new MetricFlowUnit(0, result);
    }

    protected static Field<?> getAggDimension(final Field<Double> numDimension,
                                              AggregateFunction aggregateFunction) {
        if (aggregateFunction == AggregateFunction.MAX) {
            return DSL.max(numDimension);
        } else if (aggregateFunction == AggregateFunction.MIN) {
            return DSL.min(numDimension);
        } else if (aggregateFunction == AggregateFunction.AVG) {
            return DSL.avg(numDimension);
        } else {
            return DSL.sum(numDimension);
        }
    }

  public enum AggregateFunction {
    SUM,
    MAX,
    MIN,
    AVG
  }
}
