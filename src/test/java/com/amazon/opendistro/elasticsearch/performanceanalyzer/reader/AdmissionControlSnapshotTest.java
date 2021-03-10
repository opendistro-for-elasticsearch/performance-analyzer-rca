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

import com.amazon.opendistro.elasticsearch.performanceanalyzer.reader.AdmissionControlSnapshot.Fields;
import org.jooq.BatchBindStep;
import org.jooq.Record;
import org.jooq.Result;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;

import static org.junit.Assert.assertEquals;

public class AdmissionControlSnapshotTest {

    private static final String DB_URL = "jdbc:sqlite:";
    private static final String TEST_CONTROLLER = "testController";
    private static final Long TEST_REJECTION_COUNT = 1L;
    private Connection connection;
    AdmissionControlSnapshot snapshot;

    @Before
    public void setup() throws Exception {
        Class.forName("org.sqlite.JDBC");
        System.setProperty("java.io.tmpdir", "/tmp");
        connection = DriverManager.getConnection(DB_URL);
        snapshot = new AdmissionControlSnapshot(connection, System.currentTimeMillis());
    }

    @Test
    public void testReadAdmissionControlSnapshot() throws Exception {
        final BatchBindStep handle = snapshot.startBatchPut();
        insertIntoTable(handle, TEST_CONTROLLER, TEST_REJECTION_COUNT);

        final Result<Record> result = snapshot.fetchAll();

        assertEquals(1, result.size());
        assertEquals(TEST_CONTROLLER, result.get(0).get(Fields.CONTROLLER_NAME.toString()));
        assertEquals(TEST_REJECTION_COUNT, result.get(0).get(Fields.REJECTION_COUNT.toString()));
    }

    @After
    public void tearDown() throws Exception {
        connection.close();
    }

    private void insertIntoTable(BatchBindStep handle, String controller, Long rejectionCount) {
        handle.bind(controller, rejectionCount).execute();
    }

}
