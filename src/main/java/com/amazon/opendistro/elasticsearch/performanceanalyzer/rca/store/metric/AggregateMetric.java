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
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
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
 * AggregateMetric can be used to group the sqlite from from write to one or more columns
 * and perform sum aggregation and sorting function on the result
 * For example, we can use this metric to collect the operations for cpu usage in descending
 * order by constructing this Metric as follows:
 * <p>
 * new AggregateMetric(5, CPU_Utilization.NAME, CommonDimension.OPERATION.toString());
 * </p>
 */
public class AggregateMetric extends Metric {

  private static final Logger LOG = LogManager.getLogger(AggregateMetric.class);
  public static final String NAME = AggregateMetric.class.getSimpleName();
  private final String tableName;
  private final List<String> groupByFieldsName;

  public AggregateMetric(final long evaluationIntervalSeconds, final String tableName,
      final String... groupByFieldsName) {
    super(AggregateMetric.NAME, evaluationIntervalSeconds);
    this.tableName = tableName;
    this.groupByFieldsName = new ArrayList<>(Arrays.asList(groupByFieldsName));
  }

  @Override
  public MetricFlowUnit gather(final Queryable queryable) {
    LOG.debug("Metric: Trying to gather metrics for {}", tableName);
    final Result<Record> result;
    final List<Field<?>> groupByFieldsList = new ArrayList<>();
    final List<Field<?>> fieldsList;
    try {
      final MetricsDB db = queryable.getMetricsDB();
      final DSLContext context = db.getDSLContext();
      groupByFieldsName.forEach(f -> groupByFieldsList.add(DSL.field(DSL.name(f))));
      final Field<Double> numDimension = DSL.field(DSL.name(MetricsDB.AVG), Double.class);
      final Field<BigDecimal> aggDimension = DSL.sum(numDimension);
      fieldsList = new ArrayList<>(groupByFieldsList);
      fieldsList.add(aggDimension);
      result = context
          .select(fieldsList)
          .from(tableName)
          .groupBy(groupByFieldsList)
          .orderBy(aggDimension.desc())
          .fetch();

    } catch (Exception e) {
      //TODO: Emit log/stats that gathering failed.
      LOG.error("RCA: Caught an exception while getting the DB {}", e.getMessage());
      return MetricFlowUnit.generic();
    }
    final List<List<String>> flowUnitData = new ArrayList<>();
    flowUnitData.add(fieldsList.stream()
        .map(Field::getName)
        .collect(Collectors.toList()));
    for (Record record : result) {
      flowUnitData.add(fieldsList.stream()
          .map(f -> record.getValue(f, String.class))
          .collect(Collectors.toList()));
    }
    return new MetricFlowUnit(0, flowUnitData);
  }
}
