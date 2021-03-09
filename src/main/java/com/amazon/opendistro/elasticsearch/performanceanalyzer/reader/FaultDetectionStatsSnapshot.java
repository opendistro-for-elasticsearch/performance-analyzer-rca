/*
 * Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package com.amazon.opendistro.elasticsearch.performanceanalyzer.reader;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.DBUtils;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metricsdb.MetricsDB;

import java.sql.Connection;
import java.util.*;

import org.jooq.BatchBindStep;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Record;
import org.jooq.Result;
import org.jooq.SQLDialect;
import org.jooq.SelectField;
import org.jooq.impl.DSL;

public class FaultDetectionStatsSnapshot implements Removable {

    private final DSLContext create;
    private final Long windowStartTime;
    private final String tableName;
    private List<Field<?>> columns;

    public FaultDetectionStatsSnapshot(Connection conn, Long windowStartTime) {
        this.create = DSL.using(conn, SQLDialect.SQLITE);
        this.windowStartTime = windowStartTime;
        this.tableName = "fault_detection_" + windowStartTime;

        this.columns =
                new ArrayList<Field<?>>() {
                    {
                        this.add(
                                DSL.field(
                                        DSL.name(AllMetrics.FaultDetectionMetric.FOLLOWER_CHECK_LATENCY.toString()),
                                        Double.class));
                        this.add(
                                DSL.field(
                                        DSL.name(AllMetrics.FaultDetectionMetric.FOLLOWER_CHECK_FAILURE.toString()),
                                        Double.class));
                        this.add(
                                DSL.field(
                                        DSL.name(AllMetrics.FaultDetectionMetric.LEADER_CHECK_LATENCY.toString()),
                                        Double.class));
                        this.add(
                                DSL.field(
                                        DSL.name(AllMetrics.FaultDetectionMetric.LEADER_CHECK_FAILURE.toString()),
                                        Double.class));
                    }
                };
        create.createTable(this.tableName).columns(columns).execute();
    }

    public BatchBindStep startBatchPut() {
        List<Object> dummyValues = new ArrayList<>();
        for (int i = 0; i < columns.size(); i++) {
            dummyValues.add(null);
        }
        return create.batch(create.insertInto(DSL.table(this.tableName)).values(dummyValues));
    }

    public Result<Record> fetchAll() {
        return create.select().from(DSL.table(this.tableName)).fetch();
    }

    @Override
    public void remove() throws Exception {
        create.dropTable(DSL.table(this.tableName)).execute();
    }

    public Result<Record> fetchAggregatedMetrics() {

        List<SelectField<?>> fields = new ArrayList<>();
        fields.addAll(aggregateFollowerCheckLatency());
        fields.addAll(aggregateLeaderCheckLatency());
        fields.addAll(aggregateFollowerCheckFailure());
        fields.addAll(aggregateLeaderCheckFailure());

        ArrayList<Field<?>> groupByFields =
                new ArrayList<>();

        return create.select(fields).from(DSL.table(this.tableName)).groupBy(groupByFields).fetch();
    }

    private List<SelectField<?>> aggregateFollowerCheckLatency() {
        return new ArrayList<SelectField<?>>()
        {
            {
                this.add(
                        DSL.sum(
                                DSL.field(
                                        DSL.name(
                                                AllMetrics.FaultDetectionMetric.FOLLOWER_CHECK_LATENCY
                                                        .toString()),
                                        Double.class))
                                .as(
                                        DBUtils.getAggFieldName(
                                                AllMetrics.FaultDetectionMetric.FOLLOWER_CHECK_LATENCY.toString(),
                                                MetricsDB.SUM)));

                this.add(
                        DSL.avg(
                                DSL.field(
                                        DSL.name(
                                                AllMetrics.FaultDetectionMetric.FOLLOWER_CHECK_LATENCY
                                                        .toString()),
                                        Double.class))
                                .as(
                                        DBUtils.getAggFieldName(
                                                AllMetrics.FaultDetectionMetric.FOLLOWER_CHECK_LATENCY.toString(),
                                                MetricsDB.AVG)));
                this.add(
                        DSL.min(
                                DSL.field(
                                        DSL.name(
                                                AllMetrics.FaultDetectionMetric.FOLLOWER_CHECK_LATENCY
                                                        .toString()),
                                        Double.class))
                                .as(
                                        DBUtils.getAggFieldName(
                                                AllMetrics.FaultDetectionMetric.FOLLOWER_CHECK_LATENCY.toString(),
                                                MetricsDB.MIN)));
                this.add(
                        DSL.max(
                                DSL.field(
                                        DSL.name(
                                                AllMetrics.FaultDetectionMetric.FOLLOWER_CHECK_LATENCY
                                                        .toString()),
                                        Double.class))
                                .as(
                                        DBUtils.getAggFieldName(
                                                AllMetrics.FaultDetectionMetric.FOLLOWER_CHECK_LATENCY.toString(),
                                                MetricsDB.MAX)));
            }
        };
    }

    private List<SelectField<?>> aggregateLeaderCheckLatency() {
        return new ArrayList<SelectField<?>>()
        {
            {
                this.add(
                        DSL.sum(
                                DSL.field(
                                        DSL.name(
                                                AllMetrics.FaultDetectionMetric.LEADER_CHECK_LATENCY
                                                        .toString()),
                                        Double.class))
                                .as(
                                        DBUtils.getAggFieldName(
                                                AllMetrics.FaultDetectionMetric.LEADER_CHECK_LATENCY.toString(),
                                                MetricsDB.SUM)));

                this.add(
                        DSL.avg(
                                DSL.field(
                                        DSL.name(
                                                AllMetrics.FaultDetectionMetric.LEADER_CHECK_LATENCY
                                                        .toString()),
                                        Double.class))
                                .as(
                                        DBUtils.getAggFieldName(
                                                AllMetrics.FaultDetectionMetric.LEADER_CHECK_LATENCY.toString(),
                                                MetricsDB.AVG)));
                this.add(
                        DSL.min(
                                DSL.field(
                                        DSL.name(
                                                AllMetrics.FaultDetectionMetric.LEADER_CHECK_LATENCY
                                                        .toString()),
                                        Double.class))
                                .as(
                                        DBUtils.getAggFieldName(
                                                AllMetrics.FaultDetectionMetric.LEADER_CHECK_LATENCY.toString(),
                                                MetricsDB.MIN)));
                this.add(
                        DSL.max(
                                DSL.field(
                                        DSL.name(
                                                AllMetrics.FaultDetectionMetric.LEADER_CHECK_LATENCY
                                                        .toString()),
                                        Double.class))
                                .as(
                                        DBUtils.getAggFieldName(
                                                AllMetrics.FaultDetectionMetric.LEADER_CHECK_LATENCY.toString(),
                                                MetricsDB.MAX)));
            }
        };
    }

    private List<SelectField<?>> aggregateFollowerCheckFailure() {
        return new ArrayList<SelectField<?>>()
        {
            {
                this.add(
                        DSL.sum(
                                DSL.field(
                                        DSL.name(
                                                AllMetrics.FaultDetectionMetric.FOLLOWER_CHECK_FAILURE
                                                        .toString()),
                                        Long.class))
                                .as(
                                        DBUtils.getAggFieldName(
                                                AllMetrics.FaultDetectionMetric.FOLLOWER_CHECK_FAILURE.toString(),
                                                MetricsDB.SUM)));

                this.add(
                        DSL.avg(
                                DSL.field(
                                        DSL.name(
                                                AllMetrics.FaultDetectionMetric.FOLLOWER_CHECK_FAILURE
                                                        .toString()),
                                        Double.class))
                                .as(
                                        DBUtils.getAggFieldName(
                                                AllMetrics.FaultDetectionMetric.FOLLOWER_CHECK_FAILURE.toString(),
                                                MetricsDB.AVG)));
                this.add(
                        DSL.min(
                                DSL.field(
                                        DSL.name(
                                                AllMetrics.FaultDetectionMetric.FOLLOWER_CHECK_FAILURE
                                                        .toString()),
                                        Long.class))
                                .as(
                                        DBUtils.getAggFieldName(
                                                AllMetrics.FaultDetectionMetric.FOLLOWER_CHECK_FAILURE.toString(),
                                                MetricsDB.MIN)));
                this.add(
                        DSL.max(
                                DSL.field(
                                        DSL.name(
                                                AllMetrics.FaultDetectionMetric.FOLLOWER_CHECK_FAILURE
                                                        .toString()),
                                        Long.class))
                                .as(
                                        DBUtils.getAggFieldName(
                                                AllMetrics.FaultDetectionMetric.FOLLOWER_CHECK_FAILURE.toString(),
                                                MetricsDB.MAX)));
            }
        };
    }

    private List<SelectField<?>> aggregateLeaderCheckFailure() {
        return new ArrayList<SelectField<?>>()
        {
            {
                this.add(
                        DSL.sum(
                                DSL.field(
                                        DSL.name(
                                                AllMetrics.FaultDetectionMetric.LEADER_CHECK_FAILURE
                                                        .toString()),
                                        Long.class))
                                .as(
                                        DBUtils.getAggFieldName(
                                                AllMetrics.FaultDetectionMetric.LEADER_CHECK_FAILURE.toString(),
                                                MetricsDB.SUM)));

                this.add(
                        DSL.avg(
                                DSL.field(
                                        DSL.name(
                                                AllMetrics.FaultDetectionMetric.LEADER_CHECK_FAILURE
                                                        .toString()),
                                        Double.class))
                                .as(
                                        DBUtils.getAggFieldName(
                                                AllMetrics.FaultDetectionMetric.LEADER_CHECK_FAILURE.toString(),
                                                MetricsDB.AVG)));
                this.add(
                        DSL.min(
                                DSL.field(
                                        DSL.name(
                                                AllMetrics.FaultDetectionMetric.LEADER_CHECK_FAILURE
                                                        .toString()),
                                        Long.class))
                                .as(
                                        DBUtils.getAggFieldName(
                                                AllMetrics.FaultDetectionMetric.LEADER_CHECK_FAILURE.toString(),
                                                MetricsDB.MIN)));
                this.add(
                        DSL.max(
                                DSL.field(
                                        DSL.name(
                                                AllMetrics.FaultDetectionMetric.LEADER_CHECK_FAILURE
                                                        .toString()),
                                        Long.class))
                                .as(
                                        DBUtils.getAggFieldName(
                                                AllMetrics.FaultDetectionMetric.LEADER_CHECK_FAILURE.toString(),
                                                MetricsDB.MAX)));
            }
        };
    }
}
