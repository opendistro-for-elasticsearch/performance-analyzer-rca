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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.FlowUnitMessage;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.HotNodeSummaryMessage;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.JvmEnum;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.ResourceType;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotResourceSummary.SQL_SCHEMA_CONSTANTS;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import java.util.List;
import org.jooq.Field;
import org.jooq.Record;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

public class HotNodeSummaryTest {
    private static final String NODE_ID = "ABC123";
    private static final String HOST_ADDRESS = "127.0.0.0";
    private static final ResourceType RESOURCE_TYPE = ResourceType.newBuilder().setJVM(JvmEnum.YOUNG_GEN).build();
    private static final double THRESHOLD = 3.14;
    private static final double VALUE = 2.71;
    private static HotNodeSummary uut;

    @BeforeClass
    public static void setup() {
        uut = new HotNodeSummary(NODE_ID, HOST_ADDRESS);
        uut.appendNestedSummary(new HotResourceSummary(RESOURCE_TYPE, THRESHOLD, VALUE, 0));
    }

    @Test
    public void testBuildSummaryMessage() {
        HotNodeSummaryMessage msg = uut.buildSummaryMessage();
        Assert.assertNotNull(msg);
        Assert.assertEquals(NODE_ID, msg.getNodeID());
        Assert.assertEquals(HOST_ADDRESS, msg.getHostAddress());
        Assert.assertEquals(1, uut.getHotResourceSummaryList().size());
    }

    @Test
    public void testBuildSummaryMessageAndAddToFlowUnit() {
        // No assertions need to be made here, this function is a noop in the uut
        FlowUnitMessage.Builder msgBuilder = FlowUnitMessage.newBuilder();
        uut.buildSummaryMessageAndAddToFlowUnit(msgBuilder);
        Assert.assertEquals(uut.buildSummaryMessage(), msgBuilder.getHotNodeSummary());
    }

    @Test
    public void testToString() {
        Assert.assertEquals(NODE_ID + " " + HOST_ADDRESS + " " + uut.getHotResourceSummaryList() + " " +  uut.getHotShardSummaryList(),
                uut.toString());
    }

    @Test
    public void testGetTableName() {
        Assert.assertEquals(HotNodeSummary.HOT_NODE_SUMMARY_TABLE, uut.getTableName());
    }

    @Test
    public void testGetSqlSchema() {
        List<Field<?>> schema = uut.getSqlSchema();
        Assert.assertEquals(2, schema.size());
        Assert.assertEquals(HotNodeSummary.NodeSummaryField.NODE_ID_FIELD.getField(), schema.get(0));
        Assert.assertEquals(HotNodeSummary.NodeSummaryField.HOST_IP_ADDRESS_FIELD.getField(), schema.get(1));
    }

    @Test
    public void testGetSqlValue() {
        List<Object> rows = uut.getSqlValue();
        Assert.assertEquals(2, rows.size());
        Assert.assertEquals(NODE_ID, rows.get(0));
        Assert.assertEquals(HOST_ADDRESS, rows.get(1));
    }

    @Test
    public void testToJson() {
        JsonElement elem = uut.toJson();
        Assert.assertTrue(elem.isJsonObject());
        JsonObject json = ((JsonObject) elem);
        Assert.assertEquals(NODE_ID, json.get(HotNodeSummary.SQL_SCHEMA_CONSTANTS.NODE_ID_COL_NAME).getAsString());
        Assert.assertEquals(HOST_ADDRESS, json.get(HotNodeSummary.SQL_SCHEMA_CONSTANTS.HOST_IP_ADDRESS_COL_NAME).getAsString());
        String tableName = uut.getHotResourceSummaryList().get(0).getTableName();
        JsonObject resourceJson = json.get(tableName).getAsJsonArray().get(0).getAsJsonObject();
        Assert.assertEquals(THRESHOLD, resourceJson.get(SQL_SCHEMA_CONSTANTS.THRESHOLD_COL_NAME).getAsDouble(), 0.01);
        Assert.assertEquals(VALUE, resourceJson.get(SQL_SCHEMA_CONSTANTS.VALUE_COL_NAME).getAsDouble(), 0.01);
    }

    @Test
    public void testBuildSummary() {
        Assert.assertNull(HotClusterSummary.buildSummary(null));
        Record testRecord = Mockito.mock(Record.class);
        Mockito.when(testRecord.get(HotNodeSummary.NodeSummaryField.NODE_ID_FIELD.getField(), String.class))
                .thenReturn(NODE_ID);
        Mockito.when(testRecord.get(HotNodeSummary.NodeSummaryField.HOST_IP_ADDRESS_FIELD.getField(), String.class))
                .thenReturn(HOST_ADDRESS);
        HotNodeSummary summary = HotNodeSummary.buildSummary(testRecord);
        Assert.assertNotNull(summary);
        Assert.assertEquals(NODE_ID, summary.getNodeID());
        Assert.assertEquals(HOST_ADDRESS, summary.getHostAddress());
    }
}
