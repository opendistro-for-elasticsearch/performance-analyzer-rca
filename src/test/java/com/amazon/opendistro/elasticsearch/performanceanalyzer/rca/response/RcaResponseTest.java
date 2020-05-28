/*
 * Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.response;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.isA;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.FlowUnitMessage;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.flow_units.ResourceFlowUnit;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotClusterSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotClusterSummary.SQL_SCHEMA_CONSTANTS;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotNodeSummary;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.jooq.Field;
import org.jooq.Record;
import org.jooq.exception.DataTypeException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

public class RcaResponseTest {
    private static final String RCA_NAME = "NAME";
    private static final String STATE = "STATE";
    private static final Long TIMESTAMP = 0L;
    private static final int NUM_OF_NODE = 2;
    private static final int NUM_OF_UNHEALTHY_NODE = 1;


    private RcaResponse uut;

    @Mock
    Record record;

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
        uut = new RcaResponse(RCA_NAME, STATE, TIMESTAMP);
    }

    @Test
    public void testBuildResponse() {
        Mockito.when(record
                .get(ResourceFlowUnit.ResourceFlowUnitFieldValue.RCA_NAME_FILELD.getField(), String.class))
                .thenReturn(RCA_NAME);
        Mockito.when(record
                .get(ResourceFlowUnit.ResourceFlowUnitFieldValue.STATE_NAME_FILELD.getField(), String.class))
                .thenReturn(STATE);
        Mockito.when(record
                .get(ResourceFlowUnit.ResourceFlowUnitFieldValue.TIMESTAMP_FIELD.getField(), Long.class))
                .thenReturn(TIMESTAMP);
        RcaResponse response = RcaResponse.buildResponse(record);
        Assert.assertEquals(RCA_NAME, response.getRcaName());
        Assert.assertEquals(STATE, response.getState());
        Assert.assertEquals(TIMESTAMP, (Long) response.getTimeStamp());
    }

    @Test
    public void testBuildResponseDurability() {
        // buildResponse should survive all the exceptions below
        Mockito.when(record.get(isA(Field.class), any(Class.class))).thenThrow(new IllegalArgumentException());
        Assert.assertNull(RcaResponse.buildResponse(record));
        Mockito.when(record.get(isA(Field.class), any(Class.class))).thenThrow(new DataTypeException("uh-oh"));
        Assert.assertNull(RcaResponse.buildResponse(record));
        Mockito.when(record.get(isA(Field.class), any(Class.class))).thenThrow(new NullPointerException());
        Assert.assertNull(RcaResponse.buildResponse(record));
    }

    @Test
    public void testBuildSummaryMessage() {
        Assert.assertNull(uut.buildSummaryMessage());
    }

    @Test
    public void testBuildSummaryMessageAndAddToFlowUnit() {
        // Included for coverage, this is a noop
        uut.buildSummaryMessageAndAddToFlowUnit(FlowUnitMessage.newBuilder());
    }

    @Test
    public void testBasicGetters() {
        Assert.assertEquals(ResourceFlowUnit.RCA_TABLE_NAME, uut.getTableName());
        Assert.assertNull(uut.getSqlSchema());
        Assert.assertNull(uut.getSqlValue());
    }

    @Test
    public void testToJson() {
        JsonElement jsonElement = uut.toJson();
        JsonObject obj = jsonElement.getAsJsonObject();
        Assert.assertEquals(RCA_NAME, obj
                .get(ResourceFlowUnit.SQL_SCHEMA_CONSTANTS.RCA_COL_NAME).getAsString());
        Assert.assertEquals(STATE, obj
                .get(ResourceFlowUnit.SQL_SCHEMA_CONSTANTS.STATE_COL_NAME).getAsString());
        Assert.assertEquals(TIMESTAMP, (Long) obj
                .get(ResourceFlowUnit.SQL_SCHEMA_CONSTANTS.TIMESTAMP_COL_NAME).getAsLong());
        HotClusterSummary summary = new HotClusterSummary(NUM_OF_NODE, NUM_OF_UNHEALTHY_NODE);
        uut.appendNestedSummary(summary);
        jsonElement = uut.toJson();
        obj = jsonElement.getAsJsonObject();
        Assert.assertEquals(RCA_NAME, obj
                .get(ResourceFlowUnit.SQL_SCHEMA_CONSTANTS.RCA_COL_NAME).getAsString());
        Assert.assertEquals(STATE, obj
                .get(ResourceFlowUnit.SQL_SCHEMA_CONSTANTS.STATE_COL_NAME).getAsString());
        Assert.assertEquals(TIMESTAMP, (Long) obj
                .get(ResourceFlowUnit.SQL_SCHEMA_CONSTANTS.TIMESTAMP_COL_NAME).getAsLong());
        String tableName = uut.getHotClusterSummaryList().get(0).getTableName();
        JsonObject ClusterJson = obj.get(tableName).getAsJsonArray().get(0).getAsJsonObject();
        Assert.assertEquals(NUM_OF_NODE, ClusterJson.get(SQL_SCHEMA_CONSTANTS.NUM_OF_NODES_COL_NAME).getAsInt());
        Assert.assertEquals(NUM_OF_UNHEALTHY_NODE, ClusterJson.get(SQL_SCHEMA_CONSTANTS.NUM_OF_UNHEALTHY_NODES_COL_NAME).getAsInt());
    }
}
