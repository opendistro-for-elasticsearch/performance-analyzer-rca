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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.FlowUnitMessage;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.HotShardSummaryMessage;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.GenericSummary;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import java.util.List;
import org.jooq.Field;
import org.jooq.Record;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class HotShardSummaryTest {
    private final String INDEX_NAME = "index_1";
    private final String SHARD_ID = "shard_1";
    private final String NODE_ID = "node_1";
    private final double CPU_UTILIZATION = 0.65;
    private final double CPU_UTILIZATION_THRESHOLD = 0.10;
    private final double IO_THROUGHPUT = 500000;
    private final double IO_THROUGHPUT_THRESHOLD = 250000;
    private final double IO_SYSCALLRATE = 0.232;
    private final double IO_SYSCALLRATE_THRESHOLD = 0.10;
    private final int TIME_PERIOD = 2020;

    private HotShardSummary uut;

    @Before
    public void setup() {
        uut = new HotShardSummary(INDEX_NAME, SHARD_ID, NODE_ID, TIME_PERIOD);
        uut.setcpuUtilization(CPU_UTILIZATION);
        uut.setCpuUtilizationThreshold(CPU_UTILIZATION_THRESHOLD);
        uut.setIoThroughput(IO_THROUGHPUT);
        uut.setIoThroughputThreshold(IO_THROUGHPUT_THRESHOLD);
        uut.setIoSysCallrate(IO_SYSCALLRATE);
        uut.setIoSysCallrateThreshold(IO_SYSCALLRATE_THRESHOLD);
    }

    @Test
    public void testBuildSummaryMessage() {
        HotShardSummaryMessage msg = uut.buildSummaryMessage();
        Assert.assertEquals(NODE_ID, msg.getNodeId());
        Assert.assertEquals(INDEX_NAME, msg.getIndexName());
        Assert.assertEquals(SHARD_ID, msg.getShardId());
        Assert.assertEquals(CPU_UTILIZATION, msg.getCpuUtilization(), 0);
        Assert.assertEquals(CPU_UTILIZATION_THRESHOLD, msg.getCpuUtilizationThreshold(), 0);
        Assert.assertEquals(IO_THROUGHPUT, msg.getIoThroughput(), 0);
        Assert.assertEquals(IO_THROUGHPUT_THRESHOLD, msg.getIoThroughputThreshold(), 0);
        Assert.assertEquals(IO_SYSCALLRATE, msg.getIoSysCallrate(), 0);
        Assert.assertEquals(IO_SYSCALLRATE_THRESHOLD, msg.getIoSysCallrateThreshold(), 0);
        Assert.assertEquals(TIME_PERIOD, msg.getTimePeriod());
    }

    @Test
    public void testBuildSummaryMessageAndAddToFlowUnit() {
        // No assertions need to be made here, this function is a noop in the uut
        FlowUnitMessage.Builder msgBuilder = FlowUnitMessage.newBuilder();
        uut.buildSummaryMessageAndAddToFlowUnit(msgBuilder);
        Assert.assertEquals(uut.buildSummaryMessage(), msgBuilder.getHotShardSummary());
    }

    @Test
    public void testToString() {
        String expected = String.join(" ", new String[] {
                INDEX_NAME, SHARD_ID, NODE_ID,
                String.valueOf(CPU_UTILIZATION), String.valueOf(CPU_UTILIZATION_THRESHOLD),
                String.valueOf(IO_THROUGHPUT), String.valueOf(IO_THROUGHPUT_THRESHOLD),
                String.valueOf(IO_SYSCALLRATE), String.valueOf(IO_SYSCALLRATE_THRESHOLD)
        });
        Assert.assertEquals(expected, uut.toString());
    }

    @Test
    public void testGetTableName() {
        Assert.assertEquals(HotShardSummary.HOT_SHARD_SUMMARY_TABLE, uut.getTableName());
    }

    @Test
    public void testGetSqlSchema() {
        List<Field<?>> schema = uut.getSqlSchema();
        Assert.assertEquals(10, schema.size());
        Assert.assertEquals(HotShardSummary.HotShardSummaryField.INDEX_NAME_FIELD.getField(), schema.get(0));
        Assert.assertEquals(HotShardSummary.HotShardSummaryField.SHARD_ID_FIELD.getField(), schema.get(1));
        Assert.assertEquals(HotShardSummary.HotShardSummaryField.NODE_ID_FIELD.getField(), schema.get(2));
        Assert.assertEquals(HotShardSummary.HotShardSummaryField.CPU_UTILIZATION_FIELD.getField(), schema.get(3));
        Assert.assertEquals(HotShardSummary.HotShardSummaryField.CPU_UTILIZATION_THRESHOLD_FIELD.getField(), schema.get(4));
        Assert.assertEquals(HotShardSummary.HotShardSummaryField.IO_THROUGHPUT_FIELD.getField(), schema.get(5));
        Assert.assertEquals(HotShardSummary.HotShardSummaryField.IO_THROUGHPUT_THRESHOLD_FIELD.getField(), schema.get(6));
        Assert.assertEquals(HotShardSummary.HotShardSummaryField.IO_SYSCALLRATE_FIELD.getField(), schema.get(7));
        Assert.assertEquals(HotShardSummary.HotShardSummaryField.IO_SYSCALLRATE_THRESHOLD_FIELD.getField(), schema.get(8));
        Assert.assertEquals(HotShardSummary.HotShardSummaryField.TIME_PERIOD_FIELD.getField(), schema.get(9
        ));
    }

    @Test
    public void testGetSqlValue() {
        List<Object> values = uut.getSqlValue();
        Assert.assertEquals(10, values.size());
        Assert.assertEquals(INDEX_NAME, values.get(0));
        Assert.assertEquals(SHARD_ID, values.get(1));
        Assert.assertEquals(NODE_ID, values.get(2));
        Assert.assertEquals(CPU_UTILIZATION, values.get(3));
        Assert.assertEquals(CPU_UTILIZATION_THRESHOLD, values.get(4));
        Assert.assertEquals(IO_THROUGHPUT, values.get(5));
        Assert.assertEquals(IO_THROUGHPUT_THRESHOLD, values.get(6));
        Assert.assertEquals(IO_SYSCALLRATE, values.get(7));
        Assert.assertEquals(IO_SYSCALLRATE_THRESHOLD, values.get(8));
        Assert.assertEquals(TIME_PERIOD, values.get(9));
    }

    @Test
    public void testToJson() {
        JsonElement elem = uut.toJson();
        Assert.assertTrue(elem.isJsonObject());
        JsonObject json = ((JsonObject) elem);
        Assert.assertEquals(INDEX_NAME,
                json.get(HotShardSummary.SQL_SCHEMA_CONSTANTS.INDEX_NAME_COL_NAME).getAsString());
        Assert.assertEquals(SHARD_ID,
                json.get(HotShardSummary.SQL_SCHEMA_CONSTANTS.SHARD_ID_COL_NAME).getAsString());
        Assert.assertEquals(NODE_ID,
                json.get(HotShardSummary.SQL_SCHEMA_CONSTANTS.NODE_ID_COL_NAME).getAsString());
        Assert.assertEquals(CPU_UTILIZATION,
                json.get(HotShardSummary.SQL_SCHEMA_CONSTANTS.CPU_UTILIZATION_COL_NAME).getAsDouble(), 0);
        Assert.assertEquals(CPU_UTILIZATION_THRESHOLD,
                json.get(HotShardSummary.SQL_SCHEMA_CONSTANTS.CPU_UTILIZATION_THRESHOLD_COL_NAME).getAsDouble(), 0);
        Assert.assertEquals(IO_THROUGHPUT,
                json.get(HotShardSummary.SQL_SCHEMA_CONSTANTS.IO_THROUGHPUT_COL_NAME).getAsDouble(), 0);
        Assert.assertEquals(IO_THROUGHPUT_THRESHOLD,
                json.get(HotShardSummary.SQL_SCHEMA_CONSTANTS.IO_THROUGHPUT_THRESHOLD_COL_NAME).getAsDouble(), 0);
        Assert.assertEquals(IO_SYSCALLRATE,
                json.get(HotShardSummary.SQL_SCHEMA_CONSTANTS.IO_SYSCALLRATE_COL_NAME).getAsDouble(), 0);
        Assert.assertEquals(IO_SYSCALLRATE_THRESHOLD,
                json.get(HotShardSummary.SQL_SCHEMA_CONSTANTS.IO_SYSCALLRATE_THRESHOLD_COL_NAME).getAsDouble(), 0);
        Assert.assertEquals(TIME_PERIOD,
                json.get(HotShardSummary.SQL_SCHEMA_CONSTANTS.TIME_PERIOD_COL_NAME).getAsDouble(), 0);
    }

    @Test
    public void testBuildSummary() {
        Assert.assertNull(HotShardSummary.buildSummary(null));
        Record testRecord = Mockito.mock(Record.class);
        Mockito.when(testRecord.get(HotShardSummary.HotShardSummaryField.INDEX_NAME_FIELD.getField(), String.class))
                .thenReturn(INDEX_NAME);
        Mockito.when(testRecord.get(HotShardSummary.HotShardSummaryField.SHARD_ID_FIELD.getField(), String.class))
                .thenReturn(SHARD_ID);
        Mockito.when(testRecord.get(HotShardSummary.HotShardSummaryField.NODE_ID_FIELD.getField(), String.class))
                .thenReturn(NODE_ID);
        Mockito.when(testRecord.get(HotShardSummary.HotShardSummaryField.CPU_UTILIZATION_FIELD.getField(), Double.class))
                .thenReturn(CPU_UTILIZATION);
        Mockito.when(testRecord.get(HotShardSummary.HotShardSummaryField.CPU_UTILIZATION_THRESHOLD_FIELD.getField(), Double.class))
                .thenReturn(CPU_UTILIZATION_THRESHOLD);
        Mockito.when(testRecord.get(HotShardSummary.HotShardSummaryField.IO_THROUGHPUT_FIELD.getField(), Double.class))
                .thenReturn(IO_THROUGHPUT);
        Mockito.when(testRecord.get(HotShardSummary.HotShardSummaryField.IO_THROUGHPUT_THRESHOLD_FIELD.getField(), Double.class))
                .thenReturn(IO_THROUGHPUT_THRESHOLD);
        Mockito.when(testRecord.get(HotShardSummary.HotShardSummaryField.IO_SYSCALLRATE_FIELD.getField(), Double.class))
                .thenReturn(IO_SYSCALLRATE);
        Mockito.when(testRecord.get(HotShardSummary.HotShardSummaryField.IO_SYSCALLRATE_THRESHOLD_FIELD.getField(), Double.class))
                .thenReturn(IO_SYSCALLRATE_THRESHOLD);
        Mockito.when(testRecord.get(HotShardSummary.HotShardSummaryField.TIME_PERIOD_FIELD.getField(), Integer.class))
                .thenReturn(TIME_PERIOD);
        GenericSummary summary = HotShardSummary.buildSummary(testRecord);
        Assert.assertNotNull(summary);
        List<Object> values = summary.getSqlValue();
        Assert.assertEquals(10, values.size());
        Assert.assertEquals(INDEX_NAME, values.get(0));
        Assert.assertEquals(SHARD_ID, values.get(1));
        Assert.assertEquals(NODE_ID, values.get(2));
        Assert.assertEquals(CPU_UTILIZATION, values.get(3));
        Assert.assertEquals(CPU_UTILIZATION_THRESHOLD, values.get(4));
        Assert.assertEquals(IO_THROUGHPUT, values.get(5));
        Assert.assertEquals(IO_THROUGHPUT_THRESHOLD, values.get(6));
        Assert.assertEquals(IO_SYSCALLRATE, values.get(7));
        Assert.assertEquals(IO_SYSCALLRATE_THRESHOLD, values.get(8));
        Assert.assertEquals(TIME_PERIOD, values.get(9));
    }
}
