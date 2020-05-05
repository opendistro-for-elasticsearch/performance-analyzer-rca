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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.util;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.metricsdb.MetricsDB;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.Queryable;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.reader.Removable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.Result;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;

public class SQLiteReader implements Queryable, Removable {
    private final Connection conn;
    private final DSLContext dslContext;
    private final String DBProtocol = "jdbc:sqlite:";

    public SQLiteReader(final String pathToSqlite) throws SQLException {
        conn = DriverManager.getConnection(DBProtocol + pathToSqlite);
        dslContext = DSL.using(conn, SQLDialect.SQLITE);
    }

    public DSLContext getContext() {
        return dslContext;
    }

    @Override
    public void remove() throws Exception {
        conn.close();
    }

    @Override
    public MetricsDB getMetricsDB() throws Exception {
        return new MetricsDBTest(System.currentTimeMillis());
    }

    @Override
    public Result<Record> queryMetrics(MetricsDB db, String metricName) {
        throw new IllegalArgumentException("Should not call");
    }

    @Override
    public Result<Record> queryMetrics(MetricsDB db, String metricName, String dimension, String aggregation) {
        throw new IllegalArgumentException("Should not call");
    }

    @Override
    public long getDBTimestamp(MetricsDB db) {
        return 0;
    }

    public class MetricsDBTest extends MetricsDB {

        public MetricsDBTest(long windowStartTime) throws Exception {
            super(windowStartTime);
        }

        @Override
        public DSLContext getDSLContext() {
            return dslContext;
        }
    }
}
