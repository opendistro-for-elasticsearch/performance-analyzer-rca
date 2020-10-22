/*
 *  Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License").
 *  You may not use this file except in compliance with the License.
 *  A copy of the License is located at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  or in the "license" file accompanying this file. This file is distributed
 *  on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *  express or implied. See the License for the specific language governing
 *  permissions and limitations under the License.
 */

package com.amazon.opendistro.elasticsearch.performanceanalyzer.reader;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.DBUtils;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metricsdb.MetricsDB;
import com.google.common.annotations.VisibleForTesting;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.jooq.BatchBindStep;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Record;
import org.jooq.Result;
import org.jooq.SQLDialect;
import org.jooq.SelectField;
import org.jooq.impl.DSL;

public class MasterThrottlingMetricsSnapshot implements Removable {

    private final DSLContext create;
    private final Long windowStartTime;
    private final String tableName;
    private List<Field<?>> columns;

    public MasterThrottlingMetricsSnapshot(Connection conn, Long windowStartTime) {
        this.create = DSL.using(conn, SQLDialect.SQLITE);
        this.windowStartTime = windowStartTime;
        this.tableName = "master_throttling_" + windowStartTime;

        this.columns =
                new ArrayList<Field<?>>() {
                    {
                        this.add(
                                DSL.field(
                                        DSL.name(AllMetrics.MasterThrottlingValue.DATA_RETRYING_TASK_COUNT.toString()),
                                        Long.class));
                        this.add(
                                DSL.field(
                                        DSL.name(AllMetrics.MasterThrottlingValue.MASTER_THROTTLED_PENDING_TASK_COUNT.toString()),
                                        Long.class));
                    }
                };
        create.createTable(this.tableName).columns(columns).execute();
    }

    @Override
    public void remove() throws Exception {
        create.dropTable(DSL.table(this.tableName)).execute();
    }

    /**
     * Return all master throttling metric in the current window.
     *
     * <p>Actual Table
     * Data_RetryingPendingTasksCount|Master_ThrottledPendingTasksCount
     * 5 |10
     * <p/>
     *
     * @return aggregated master task
     */
    public Result<Record> fetchAll() {
        return create.select().from(DSL.table(this.tableName)).fetch();
    }

    public BatchBindStep startBatchPut() {
        List<Object> dummyValues = new ArrayList<>();
        for (int i = 0; i < columns.size(); i++) {
            dummyValues.add(null);
        }
        return create.batch(create.insertInto(DSL.table(this.tableName)).values(dummyValues));
    }

    /**
     * Return one row per master throttling metric. It has 8 columns
     *
     * <p>|SUM_MASTER_THROTTLED_COUNT|AVG_MASTER_THROTTLED_COUNT|MIN_MASTER_THROTTLED_COUNT|MAX_MASTER_THROTTLED_COUNT|
     * SUM_DATA_RETRYING_COUNT|AVG_DATA_RETRYING_COUNT|MIN_DATA_RETRYING_COUNT|MAX_DATA_RETRYING_COUNT
     * <p/>
     *
     * @return aggregated master task
     */
    public Result<Record> fetchAggregatedMetrics() {

        List<SelectField<?>> fields =
                new ArrayList<SelectField<?>>() {
                    {
                        this.add(
                                DSL.sum(
                                        DSL.field(
                                                DSL.name(
                                                        AllMetrics.MasterThrottlingValue.MASTER_THROTTLED_PENDING_TASK_COUNT
                                                                .toString()),
                                                Long.class))
                                        .as(
                                                DBUtils.getAggFieldName(
                                                        AllMetrics.MasterThrottlingValue.MASTER_THROTTLED_PENDING_TASK_COUNT.toString(),
                                                        MetricsDB.SUM)));
                        this.add(
                                DSL.avg(
                                        DSL.field(
                                                DSL.name(
                                                        AllMetrics.MasterThrottlingValue.MASTER_THROTTLED_PENDING_TASK_COUNT
                                                                .toString()),
                                                Double.class))
                                        .as(
                                                DBUtils.getAggFieldName(
                                                        AllMetrics.MasterThrottlingValue.MASTER_THROTTLED_PENDING_TASK_COUNT.toString(),
                                                        MetricsDB.AVG)));
                        this.add(
                                DSL.min(
                                        DSL.field(
                                                DSL.name(
                                                        AllMetrics.MasterThrottlingValue.MASTER_THROTTLED_PENDING_TASK_COUNT
                                                                .toString()),
                                                Long.class))
                                        .as(
                                                DBUtils.getAggFieldName(
                                                        AllMetrics.MasterThrottlingValue.MASTER_THROTTLED_PENDING_TASK_COUNT.toString(),
                                                        MetricsDB.MIN)));
                        this.add(
                                DSL.max(
                                        DSL.field(
                                                DSL.name(
                                                        AllMetrics.MasterThrottlingValue.MASTER_THROTTLED_PENDING_TASK_COUNT
                                                                .toString()),
                                                Long.class))
                                        .as(
                                                DBUtils.getAggFieldName(
                                                        AllMetrics.MasterThrottlingValue.MASTER_THROTTLED_PENDING_TASK_COUNT.toString(),
                                                        MetricsDB.MAX)));

                        this.add(
                                DSL.sum(
                                        DSL.field(
                                                DSL.name(
                                                        AllMetrics.MasterThrottlingValue.DATA_RETRYING_TASK_COUNT
                                                                .toString()),
                                                Long.class))
                                        .as(
                                                DBUtils.getAggFieldName(
                                                        AllMetrics.MasterThrottlingValue.DATA_RETRYING_TASK_COUNT.toString(),
                                                        MetricsDB.SUM)));
                        this.add(
                                DSL.avg(
                                        DSL.field(
                                                DSL.name(
                                                        AllMetrics.MasterThrottlingValue.DATA_RETRYING_TASK_COUNT
                                                                .toString()),
                                                Double.class))
                                        .as(
                                                DBUtils.getAggFieldName(
                                                        AllMetrics.MasterThrottlingValue.DATA_RETRYING_TASK_COUNT.toString(),
                                                        MetricsDB.AVG)));
                        this.add(
                                DSL.min(
                                        DSL.field(
                                                DSL.name(
                                                        AllMetrics.MasterThrottlingValue.DATA_RETRYING_TASK_COUNT
                                                                .toString()),
                                                Long.class))
                                        .as(
                                                DBUtils.getAggFieldName(
                                                        AllMetrics.MasterThrottlingValue.DATA_RETRYING_TASK_COUNT.toString(),
                                                        MetricsDB.MIN)));
                        this.add(
                                DSL.max(
                                        DSL.field(
                                                DSL.name(
                                                        AllMetrics.MasterThrottlingValue.DATA_RETRYING_TASK_COUNT
                                                                .toString()),
                                                Long.class))
                                        .as(
                                                DBUtils.getAggFieldName(
                                                        AllMetrics.MasterThrottlingValue.DATA_RETRYING_TASK_COUNT.toString(),
                                                        MetricsDB.MAX)));
                    }
                };
        ArrayList<Field<?>> groupByFields =
                new ArrayList<Field<?>>();

        return create.select(fields).from(DSL.table(this.tableName)).groupBy(groupByFields).fetch();
    }

    @VisibleForTesting
    public void putMetrics(long retrying_task, Map<String, String> dimensions) {
        Map<Field<?>, String> dimensionMap = new HashMap<>();
        for (Map.Entry<String, String> dimension : dimensions.entrySet()) {
            dimensionMap.put(DSL.field(DSL.name(dimension.getKey()), String.class), dimension.getValue());
        }
        create
                .insertInto(DSL.table(this.tableName))
                .set(DSL.field(DSL.name(AllMetrics.MasterThrottlingValue.DATA_RETRYING_TASK_COUNT.toString()), Long.class),
                        retrying_task)
                .set(DSL.field(DSL.name(AllMetrics.MasterThrottlingValue.MASTER_THROTTLED_PENDING_TASK_COUNT.toString()), Long.class),
                        0L)
                .set(dimensionMap)
                .execute();
    }
}

