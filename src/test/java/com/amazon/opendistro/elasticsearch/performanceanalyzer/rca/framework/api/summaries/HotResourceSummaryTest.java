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
import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.HotResourceSummaryMessage;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.JvmEnum;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.ResourceType;
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

public class HotResourceSummaryTest {
    private final ResourceType RESOURCE_TYPE = ResourceType.newBuilder().setJVM(JvmEnum.YOUNG_GEN).build();
    private final double THRESHOLD = 2.718;
    private final double VALUE = 3.14159;
    private final double AVG_VALUE = 1.414;
    private final double MIN_VALUE = 0;
    private final double MAX_VALUE = 6.022;
    private final int TIME_PERIOD = 2020;
    private final String META_DATA = "128G JVM";
    private final String CONSUMER_NAME = "CONSUMER";
    private final double CONSUMER_VALUE = 8.854;

    private HotResourceSummary uut;

    @Before
    public void setup() {
        uut = new HotResourceSummary(RESOURCE_TYPE, THRESHOLD, VALUE, TIME_PERIOD, META_DATA);
        uut.setValueDistribution(MIN_VALUE, MAX_VALUE, AVG_VALUE);
        uut.appendNestedSummary(new TopConsumerSummary(CONSUMER_NAME, CONSUMER_VALUE));
    }

    @Test
    public void testBuildSummaryMessage() {
        Assert.assertEquals(1, uut.getNestedSummaryList().size());
        HotResourceSummaryMessage msg = uut.buildSummaryMessage();
        Assert.assertEquals(RESOURCE_TYPE, msg.getResourceType());
        Assert.assertEquals(THRESHOLD, msg.getThreshold(), 0);
        Assert.assertEquals(VALUE, msg.getValue(), 0);
        Assert.assertEquals(AVG_VALUE, msg.getAvgValue(), 0);
        Assert.assertEquals(MIN_VALUE, msg.getMinValue(), 0);
        Assert.assertEquals(MAX_VALUE, msg.getMaxValue(), 0);
        Assert.assertEquals(TIME_PERIOD, msg.getTimePeriod());
        Assert.assertEquals(META_DATA, msg.getMetaData());
        Assert.assertEquals(1, msg.getConsumers().getConsumerCount());
        Assert.assertEquals(CONSUMER_NAME, msg.getConsumers().getConsumer(0).getName());
        Assert.assertEquals(CONSUMER_VALUE, msg.getConsumers().getConsumer(0).getValue(), 0);
    }

    @Test
    public void testBuildSummaryMessageAndAddToFlowUnit() {
        // No assertions need to be made here, this function is a noop in the uut
        FlowUnitMessage.Builder msgBuilder = FlowUnitMessage.newBuilder();
        uut.buildSummaryMessageAndAddToFlowUnit(msgBuilder);
        Assert.assertEquals(uut.buildSummaryMessage(), msgBuilder.getHotResourceSummary());
    }

    @Test
    public void testBuildHotResourceSummaryFromMessage() {
        HotResourceSummaryMessage msg = uut.buildSummaryMessage();
        Assert.assertEquals(RESOURCE_TYPE, msg.getResourceType());
        Assert.assertEquals(THRESHOLD, msg.getThreshold(), 0);
        Assert.assertEquals(VALUE, msg.getValue(), 0);
        Assert.assertEquals(AVG_VALUE, msg.getAvgValue(), 0);
        Assert.assertEquals(MIN_VALUE, msg.getMinValue(), 0);
        Assert.assertEquals(MAX_VALUE, msg.getMaxValue(), 0);
        Assert.assertEquals(TIME_PERIOD, msg.getTimePeriod());
        Assert.assertEquals(META_DATA, msg.getMetaData());
        Assert.assertEquals(1, msg.getConsumers().getConsumerCount());
        Assert.assertEquals(CONSUMER_NAME, msg.getConsumers().getConsumer(0).getName());
        Assert.assertEquals(CONSUMER_VALUE, msg.getConsumers().getConsumer(0).getValue(), 0);
    }

    @Test
    public void testToString() {
        String expected = ResourceTypeUtil.getResourceTypeName(RESOURCE_TYPE) + " " + THRESHOLD + " " + VALUE
                + " " + ResourceTypeUtil.getResourceTypeUnit(RESOURCE_TYPE) + " " + uut.getNestedSummaryList()
                + " " + META_DATA;
        Assert.assertEquals(expected, uut.toString());
    }

    @Test
    public void testGetTableName() {
        Assert.assertEquals(HotResourceSummary.HOT_RESOURCE_SUMMARY_TABLE, uut.getTableName());
    }

    @Test
    public void testGetSqlSchema() {
        List<Field<?>> schema = uut.getSqlSchema();
        Assert.assertEquals(9, schema.size());
        Assert.assertEquals(HotResourceSummary.ResourceSummaryField.RESOURCE_TYPE_FIELD.getField(), schema.get(0));
        Assert.assertEquals(HotResourceSummary.ResourceSummaryField.THRESHOLD_FIELD.getField(), schema.get(1));
        Assert.assertEquals(HotResourceSummary.ResourceSummaryField.VALUE_FIELD.getField(), schema.get(2));
        Assert.assertEquals(HotResourceSummary.ResourceSummaryField.AVG_VALUE_FIELD.getField(), schema.get(3));
        Assert.assertEquals(HotResourceSummary.ResourceSummaryField.MIN_VALUE_FIELD.getField(), schema.get(4));
        Assert.assertEquals(HotResourceSummary.ResourceSummaryField.MAX_VALUE_FIELD.getField(), schema.get(5));
        Assert.assertEquals(HotResourceSummary.ResourceSummaryField.UNIT_TYPE_FIELD.getField(), schema.get(6));
        Assert.assertEquals(HotResourceSummary.ResourceSummaryField.TIME_PERIOD_FIELD.getField(), schema.get(7));
    }

    @Test
    public void testGetSqlValue() {
        List<Object> values = uut.getSqlValue();
        Assert.assertEquals(9, values.size());
        Assert.assertEquals(ResourceTypeUtil.getResourceTypeName(RESOURCE_TYPE), values.get(0));
        Assert.assertEquals(THRESHOLD, values.get(1));
        Assert.assertEquals(VALUE, values.get(2));
        Assert.assertEquals(AVG_VALUE, values.get(3));
        Assert.assertEquals(MIN_VALUE, values.get(4));
        Assert.assertEquals(MAX_VALUE, values.get(5));
        Assert.assertEquals(ResourceTypeUtil.getResourceTypeUnit(RESOURCE_TYPE), values.get(6));
        Assert.assertEquals(TIME_PERIOD, values.get(7));
        Assert.assertEquals(META_DATA, values.get(8));
    }

    @Test
    public void testToJson() {
        JsonElement elem = uut.toJson();
        Assert.assertTrue(elem.isJsonObject());
        JsonObject json = ((JsonObject) elem);
        Assert.assertEquals(ResourceTypeUtil.getResourceTypeName(RESOURCE_TYPE),
                json.get(HotResourceSummary.SQL_SCHEMA_CONSTANTS.RESOURCE_TYPE_COL_NAME).getAsString());
        Assert.assertEquals(THRESHOLD,
                json.get(HotResourceSummary.SQL_SCHEMA_CONSTANTS.THRESHOLD_COL_NAME).getAsDouble(), 0);
        Assert.assertEquals(VALUE,
                json.get(HotResourceSummary.SQL_SCHEMA_CONSTANTS.VALUE_COL_NAME).getAsDouble(), 0);
        Assert.assertEquals(AVG_VALUE,
                json.get(HotResourceSummary.SQL_SCHEMA_CONSTANTS.AVG_VALUE_COL_NAME).getAsDouble(), 0);
        Assert.assertEquals(MIN_VALUE,
                json.get(HotResourceSummary.SQL_SCHEMA_CONSTANTS.MIN_VALUE_COL_NAME).getAsDouble(), 0);
        Assert.assertEquals(MAX_VALUE,
                json.get(HotResourceSummary.SQL_SCHEMA_CONSTANTS.MAX_VALUE_COL_NAME).getAsDouble(), 0);
        Assert.assertEquals(ResourceTypeUtil.getResourceTypeUnit(RESOURCE_TYPE),
                json.get(HotResourceSummary.SQL_SCHEMA_CONSTANTS.UNIT_TYPE_COL_NAME).getAsString());
        Assert.assertEquals(TIME_PERIOD,
                json.get(HotResourceSummary.SQL_SCHEMA_CONSTANTS.TIME_PERIOD_COL_NAME).getAsDouble(), 0);
        Assert.assertEquals(META_DATA,
                json.get(HotResourceSummary.SQL_SCHEMA_CONSTANTS.META_DATA_COL_NAME).getAsString());
        String tableName = uut.getTopConsumerSummaryList().get(0).getTableName();
        JsonObject consumerJson = json.get(tableName).getAsJsonArray().get(0).getAsJsonObject();
        Assert.assertEquals(CONSUMER_NAME,
            consumerJson.get(TopConsumerSummary.SQL_SCHEMA_CONSTANTS.CONSUMER_NAME_COL_NAME).getAsString());
        Assert.assertEquals(CONSUMER_VALUE,
            consumerJson.get(TopConsumerSummary.SQL_SCHEMA_CONSTANTS.CONSUMER_VALUE_COL_NAME).getAsDouble(), 0.01);
    }

    @Test
    public void testBuildSummary() {
        Assert.assertNull(HotResourceSummary.buildSummary(null));
        Record testRecord = Mockito.mock(Record.class);
        Mockito.when(
                testRecord.get(HotResourceSummary.ResourceSummaryField.RESOURCE_TYPE_FIELD.getField(), String.class))
                .thenReturn(ResourceTypeUtil.getResourceTypeName(RESOURCE_TYPE));
        Mockito.when(testRecord.get(HotResourceSummary.ResourceSummaryField.THRESHOLD_FIELD.getField(), Double.class))
                .thenReturn(THRESHOLD);
        Mockito.when(testRecord.get(HotResourceSummary.ResourceSummaryField.VALUE_FIELD.getField(), Double.class))
                .thenReturn(VALUE);
        Mockito.when(testRecord.get(HotResourceSummary.ResourceSummaryField.AVG_VALUE_FIELD.getField(), Double.class))
                .thenReturn(AVG_VALUE);
        Mockito.when(testRecord.get(HotResourceSummary.ResourceSummaryField.MIN_VALUE_FIELD.getField(), Double.class))
                .thenReturn(MIN_VALUE);
        Mockito.when(testRecord.get(HotResourceSummary.ResourceSummaryField.MAX_VALUE_FIELD.getField(), Double.class))
                .thenReturn(MAX_VALUE);
        Mockito.when(testRecord.get(HotResourceSummary.ResourceSummaryField.TIME_PERIOD_FIELD.getField(), Integer.class))
                .thenReturn(TIME_PERIOD);
        GenericSummary summary = HotResourceSummary.buildSummary(testRecord);
        Assert.assertNotNull(summary);
        List<Object> values = summary.getSqlValue();
        Assert.assertEquals(9, values.size());
        Assert.assertEquals(ResourceTypeUtil.getResourceTypeName(RESOURCE_TYPE), values.get(0));
        Assert.assertEquals(THRESHOLD, values.get(1));
        Assert.assertEquals(VALUE, values.get(2));
        Assert.assertEquals(AVG_VALUE, values.get(3));
        Assert.assertEquals(MIN_VALUE, values.get(4));
        Assert.assertEquals(MAX_VALUE, values.get(5));
        Assert.assertEquals(ResourceTypeUtil.getResourceTypeUnit(RESOURCE_TYPE), values.get(6));
        Assert.assertEquals(TIME_PERIOD, values.get(7));
    }
}
