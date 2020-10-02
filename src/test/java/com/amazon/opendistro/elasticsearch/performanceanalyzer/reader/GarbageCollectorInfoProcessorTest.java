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
import static org.junit.Assert.assertTrue;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.collectors.GCInfoCollector.GCInfo;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.PerformanceAnalyzerMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.reader.GarbageCollectorInfoSnapshot.Fields;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.reader_writer_shared.Event;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.NavigableMap;
import java.util.TreeMap;
import org.jooq.Record;
import org.jooq.Result;
import org.junit.Before;
import org.junit.Test;

public class GarbageCollectorInfoProcessorTest {

  private static final String DB_URL = "jdbc:sqlite:";
  private static final String TEST_MEM_POOL = "testMemPool";
  private static final String COLLECTOR_NAME = "testCollectorName";
  private static final String GC_INFO_KEY = "gc_info";
  private GarbageCollectorInfoProcessor gcProcessor;
  private long currTimestamp;
  private NavigableMap<Long, GarbageCollectorInfoSnapshot> snapMap;
  Connection conn;

  @Before
  public void setup() throws Exception {
    Class.forName("org.sqlite.JDBC");
    System.setProperty("java.io.tmpdir", "/tmp");
    conn = DriverManager.getConnection(DB_URL);
    this.currTimestamp = System.currentTimeMillis();
    this.snapMap = new TreeMap<>();
    this.gcProcessor = GarbageCollectorInfoProcessor
        .buildGarbageCollectorInfoProcessor(currTimestamp, conn, snapMap);
  }

  @Test
  public void testHandleEvent() throws Exception {
    Event testEvent = buildTestGcInfoEvent();

    gcProcessor.initializeProcessing(currTimestamp, System.currentTimeMillis());

    assertTrue(gcProcessor.shouldProcessEvent(testEvent));

    gcProcessor.processEvent(testEvent);
    gcProcessor.finalizeProcessing();

    GarbageCollectorInfoSnapshot snap = snapMap.get(currTimestamp);
    Result<Record> result = snap.fetchAll();
    assertEquals(1, result.size());
    assertEquals(TEST_MEM_POOL, result.get(0).get(Fields.MEMORY_POOL.toString()));
    assertEquals(COLLECTOR_NAME, result.get(0).get(Fields.COLLECTOR_NAME.toString()));
  }

  private Event buildTestGcInfoEvent() {
    StringBuilder val = new StringBuilder();
    val.append(PerformanceAnalyzerMetrics.getJsonCurrentMilliSeconds())
       .append(PerformanceAnalyzerMetrics.sMetricNewLineDelimitor);

    val.append(new GCInfo(TEST_MEM_POOL, COLLECTOR_NAME).serialize())
       .append(PerformanceAnalyzerMetrics.sMetricNewLineDelimitor);
    return new Event(GC_INFO_KEY, val.toString(), System.currentTimeMillis());
  }
}