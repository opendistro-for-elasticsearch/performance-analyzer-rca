/*
 * Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

import static org.junit.Assert.assertEquals;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.reader.GarbageCollectorInfoSnapshot.Fields;
import java.sql.Connection;
import java.sql.DriverManager;
import org.jooq.BatchBindStep;
import org.jooq.Record;
import org.jooq.Result;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class GarbageCollectorInfoSnapshotTest {

  private static final String DB_URL = "jdbc:sqlite:";
  private static final String TEST_MEM_POOL = "OldGen";
  private static final String COLLECTOR_NAME = "G1";
  private Connection conn;
  GarbageCollectorInfoSnapshot snapshot;

  @Before
  public void setup() throws Exception {
    Class.forName("org.sqlite.JDBC");
    System.setProperty("java.io.tmpdir", "/tmp");
    conn = DriverManager.getConnection(DB_URL);
    snapshot = new GarbageCollectorInfoSnapshot(conn, System.currentTimeMillis());
  }

  @Test
  public void testReadGcSnapshot() throws Exception {
    final BatchBindStep handle = snapshot.startBatchPut();
    insertIntoTable(handle, TEST_MEM_POOL, COLLECTOR_NAME);

    final Result<Record> result = snapshot.fetchAll();

    assertEquals(1, result.size());
    assertEquals(TEST_MEM_POOL, result.get(0).get(Fields.MEMORY_POOL.toString()));
    assertEquals(COLLECTOR_NAME, result.get(0).get(Fields.COLLECTOR_NAME.toString()));
  }

  @After
  public void tearDown() throws Exception {
    conn.close();
  }

  private void insertIntoTable(BatchBindStep handle, String memPool, String collectorName) {
    Object[] bindVals = new Object[2];
    bindVals[0] = memPool;
    bindVals[1] = collectorName;
    handle.bind(bindVals).execute();
  }
}