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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.reader;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.DBUtils;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.FaultDetectionDimension;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metricsdb.MetricsDB;
import com.google.common.annotations.VisibleForTesting;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.BatchBindStep;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Record;
import org.jooq.Result;
import org.jooq.SQLDialect;
import org.jooq.SelectField;
import org.jooq.SelectHavingStep;
import org.jooq.impl.DSL;

public class FaultDetectionMetricsSnapshot implements Removable {
    private static final Logger LOG = LogManager.getLogger(FaultDetectionMetricsSnapshot.class);
    private final DSLContext create;
    private final Long windowStartTime;
    private final String tableName;
    private ArrayList<Field<?>> columns;
    private static final Long EXPIRE_AFTER = 600000L;

    public enum Fields {
        RID("rid"),
        FAULT_DETECTION_TYPE("type"),
        ST("st"),
        ET("et"),
        LAT("lat"),
        FAULT("fault");

        private final String fieldValue;

        Fields(String fieldValue) {
            this.fieldValue = fieldValue;
        }

        @Override
        public String toString() {
            return fieldValue;
        }

    }

    public FaultDetectionMetricsSnapshot(Connection conn, Long windowStartTime) {
        this.create = DSL.using(conn, SQLDialect.SQLITE);
        this.windowStartTime = windowStartTime;
        this.tableName = "fault_detection_" + windowStartTime;

        this.columns =
                new ArrayList<Field<?>>() {
                    {
                        this.add(DSL.field(DSL.name(Fields.RID.toString()), String.class));
                        this.add(DSL.field(DSL.name(FaultDetectionDimension.SOURCE_NODE_ID.toString()), String.class));
                        this.add(DSL.field(DSL.name(FaultDetectionDimension.TARGET_NODE_ID.toString()), String.class));
                        this.add(DSL.field(DSL.name(Fields.FAULT_DETECTION_TYPE.toString()), String.class));
                        this.add(DSL.field(DSL.name(Fields.ST.toString()), Long.class));
                        this.add(DSL.field(DSL.name(Fields.ET.toString()), Long.class));
                        this.add(DSL.field(DSL.name(Fields.FAULT.toString()), Integer.class));
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

    @VisibleForTesting
    public void putStartMetric(Long startTime, Map<String, String> dimensions) {
        Map<Field<?>, String> dimensionMap = new HashMap<>();
        for (Map.Entry<String, String> dimension : dimensions.entrySet()) {
            dimensionMap.put(DSL.field(DSL.name(dimension.getKey()), String.class), dimension.getValue());
        }
        create
                .insertInto(DSL.table(this.tableName))
                .set(DSL.field(DSL.name(Fields.ST.toString()), Long.class), startTime)
                .set(dimensionMap)
                .execute();
    }

    @VisibleForTesting
    public void putEndMetric(Long endTime, int error, Map<String, String> dimensions) {
        Map<Field<?>, String> dimensionMap = new HashMap<>();
        for (Map.Entry<String, String> dimension : dimensions.entrySet()) {
            dimensionMap.put(DSL.field(DSL.name(dimension.getKey()), String.class), dimension.getValue());
        }
        create
                .insertInto(DSL.table(this.tableName))
                .set(DSL.field(DSL.name(Fields.ET.toString()), Long.class), endTime)
                .set(DSL.field(DSL.name(Fields.FAULT.toString()), Integer.class), error)
                .set(dimensionMap)
                .execute();
    }

    public Result<Record> fetchAll() {
        return create.select().from(DSL.table(this.tableName)).fetch();
    }

    @Override
    public void remove() throws Exception {
        create.dropTable(DSL.table(this.tableName)).execute();
    }

    public void rolloverInFlightRequests(FaultDetectionMetricsSnapshot prevSnap) {
        // Fetch all entries that have not ended and write to current table.
        create
                .insertInto(DSL.table(this.tableName))
                .select(create.select().from(prevSnap.fetchInFlightRequests()))
                .execute();
    }

    public SelectHavingStep<Record> fetchInFlightRequests() {
        ArrayList<SelectField<?>> fields =
                new ArrayList<SelectField<?>>() {
                    {
                        this.add(DSL.field(DSL.name(Fields.RID.toString()), String.class));
                        this.add(DSL.field(DSL.name(FaultDetectionDimension.SOURCE_NODE_ID.toString()), String.class));
                        this.add(DSL.field(DSL.name(FaultDetectionDimension.TARGET_NODE_ID.toString()), String.class));
                        this.add(DSL.field(DSL.name(Fields.FAULT_DETECTION_TYPE.toString()), String.class));
                        this.add(DSL.field(DSL.name(Fields.FAULT.toString()), String.class));
                        this.add(DSL.field(Fields.ST.toString(), Long.class));
                        this.add(DSL.field(Fields.ET.toString(), Long.class));
                    }
                };

        return create
                .select(fields)
                .from(groupByRidAndTypeSelect())
                .where(
                        DSL.field(Fields.ST.toString())
                                .isNotNull()
                                .and(DSL.field(Fields.ET.toString()).isNull())
                                .and(DSL.field(Fields.ST.toString()).gt(this.windowStartTime - EXPIRE_AFTER)));
    }

    public SelectHavingStep<Record> groupByRidAndTypeSelect() {
        ArrayList<SelectField<?>> fields =
                new ArrayList<SelectField<?>>() {
                    {
                        this.add(DSL.field(DSL.name(Fields.RID.toString()), String.class));
                        this.add(DSL.field(DSL.name(FaultDetectionDimension.SOURCE_NODE_ID.toString()), String.class));
                        this.add(DSL.field(DSL.name(FaultDetectionDimension.TARGET_NODE_ID.toString()), String.class));
                        this.add(DSL.field(DSL.name(Fields.FAULT_DETECTION_TYPE.toString()), String.class));
                    }
                };
        fields.add(
                DSL.max(DSL.field(Fields.ST.toString(), Long.class)).as(DSL.name(Fields.ST.toString())));
        fields.add(
                DSL.max(DSL.field(Fields.ET.toString(), Long.class)).as(DSL.name(Fields.ET.toString())));
        fields.add(
                DSL.max(DSL.field(Fields.FAULT.toString(), Integer.class)).as(DSL.name(Fields.FAULT.toString())));
        return create
                .select(fields)
                .from(DSL.table(this.tableName))
                .groupBy(DSL.field(Fields.RID.toString()), DSL.field(Fields.FAULT_DETECTION_TYPE.toString()));
    }

    public SelectHavingStep<Record> fetchLatencyTable() {
        ArrayList<SelectField<?>> fields =
                new ArrayList<SelectField<?>>() {
                    {
                        this.add(DSL.field(DSL.name(Fields.RID.toString()), String.class));
                        this.add(DSL.field(DSL.name(FaultDetectionDimension.SOURCE_NODE_ID.toString()), String.class));
                        this.add(DSL.field(DSL.name(FaultDetectionDimension.TARGET_NODE_ID.toString()), Long.class));
                        this.add(DSL.field(DSL.name(Fields.FAULT_DETECTION_TYPE.toString()), String.class));
                        this.add(DSL.field(Fields.ST.toString(), Long.class));
                        this.add(DSL.field(Fields.ET.toString(), Long.class));
                        this.add(DSL.field(Fields.FAULT.toString(), Integer.class));
                    }
                };
        fields.add(
                DSL.field(Fields.ET.toString())
                        .minus(DSL.field(Fields.ST.toString()))
                        .as(DSL.name(Fields.LAT.toString())));
        return create
                .select(fields)
                .from(groupByRidAndTypeSelect())
                .where(
                        DSL.field(Fields.ET.toString())
                                .isNotNull()
                                .and(DSL.field(Fields.ST.toString()).isNotNull()));
    }

    public Result<Record> fetchAggregatedTable() {
        ArrayList<SelectField<?>> fields =
                new ArrayList<SelectField<?>>() {
                    {
                        this.add(DSL.field(DSL.name(FaultDetectionDimension.SOURCE_NODE_ID.toString()), String.class));
                        this.add(DSL.field(DSL.name(FaultDetectionDimension.TARGET_NODE_ID.toString()), String.class));
                        this.add(DSL.field(DSL.name(Fields.FAULT_DETECTION_TYPE.toString()), String.class));

                        this.add(
                                DSL.sum(DSL.field(DSL.name(Fields.LAT.toString()), Double.class))
                                        .as(DBUtils.getAggFieldName(Fields.LAT.toString(), MetricsDB.SUM)));
                        this.add(
                                DSL.avg(DSL.field(DSL.name(Fields.LAT.toString()), Double.class))
                                        .as(DBUtils.getAggFieldName(Fields.LAT.toString(), MetricsDB.AVG)));
                        this.add(
                                DSL.min(DSL.field(DSL.name(Fields.LAT.toString()), Double.class))
                                        .as(DBUtils.getAggFieldName(Fields.LAT.toString(), MetricsDB.MIN)));
                        this.add(
                                DSL.max(DSL.field(DSL.name(Fields.LAT.toString()), Double.class))
                                        .as(DBUtils.getAggFieldName(Fields.LAT.toString(), MetricsDB.MAX)));

                        this.add(
                                DSL.sum(DSL.field(DSL.name(Fields.FAULT.toString()), Double.class))
                                        .as(DBUtils.getAggFieldName(Fields.FAULT.toString(), MetricsDB.SUM)));
                        this.add(
                                DSL.avg(DSL.field(DSL.name(Fields.FAULT.toString()), Double.class))
                                        .as(DBUtils.getAggFieldName(Fields.FAULT.toString(), MetricsDB.AVG)));
                        this.add(
                                DSL.min(DSL.field(DSL.name(Fields.FAULT.toString()), Double.class))
                                        .as(DBUtils.getAggFieldName(Fields.FAULT.toString(), MetricsDB.MIN)));
                        this.add(
                                DSL.max(DSL.field(DSL.name(Fields.FAULT.toString()), Double.class))
                                        .as(DBUtils.getAggFieldName(Fields.FAULT.toString(), MetricsDB.MAX)));
                    }
                };
        ArrayList<Field<?>> groupByFields =
                new ArrayList<Field<?>>() {
                    {
                        this.add(DSL.field(DSL.name(FaultDetectionDimension.SOURCE_NODE_ID.toString()), String.class));
                        this.add(DSL.field(DSL.name(FaultDetectionDimension.TARGET_NODE_ID.toString()), String.class));
                        this.add(DSL.field(DSL.name(Fields.FAULT_DETECTION_TYPE.toString()), String.class));
                    }
                };

        return create.select(fields).from(fetchLatencyTable()).groupBy(groupByFields).fetch();
    }
}
