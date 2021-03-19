/*
 * Copyright 2021 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.AdmissionControlDimension;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.AdmissionControlValue;
import org.jooq.BatchBindStep;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Record;
import org.jooq.Result;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;

public class AdmissionControlSnapshot implements Removable {

    private final DSLContext create;
    private final String tableName;
    private List<Field<?>> columns;

    public AdmissionControlSnapshot(Connection conn, Long windowStartTime) {
        this.create = DSL.using(conn, SQLDialect.SQLITE);
        this.tableName = "admission_control_" + windowStartTime;
        this.columns = new ArrayList<Field<?>>() {
            {
                this.add(DSL.field(DSL.name(Fields.CONTROLLER_NAME.toString()), String.class));
                this.add(DSL.field(DSL.name(Fields.REJECTION_COUNT.toString()), Long.class));
            }
        };
        create.createTable(tableName).columns(columns).execute();
    }

    public DSLContext getDSLContext() {
        return create;
    }

    public BatchBindStep startBatchPut() {
        List<Object> dummyValues = new ArrayList<>();
        for (int i = 0; i < columns.size(); i++) {
            dummyValues.add(null);
        }
        return create.batch(create.insertInto(DSL.table(this.tableName)).values(dummyValues));
    }

    public Result<Record> fetchAll() {
        return create.select().from(DSL.table(tableName)).fetch();
    }

    @Override
    public void remove() throws Exception {
        create.dropTable(DSL.table(tableName)).execute();
    }

    public enum Fields {
        CONTROLLER_NAME(AdmissionControlDimension.CONTROLLER_NAME.toString()),
        REJECTION_COUNT(AdmissionControlValue.REJECTION_COUNT.toString());

        private final String fieldValue;

        Fields(String fieldValue) {
            this.fieldValue = fieldValue;
        }

        @Override
        public String toString() {
            return fieldValue;
        }
    }
}
