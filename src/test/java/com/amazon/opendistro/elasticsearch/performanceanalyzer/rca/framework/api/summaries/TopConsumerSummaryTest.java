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
import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.TopConsumerSummaryMessage;
import java.util.List;
import org.jooq.Field;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class TopConsumerSummaryTest {
    private static final String NAME = "TOP_CONSUMER";
    private static final double VALUE = 2.718;

    private static TopConsumerSummary uut;

    @BeforeClass
    public static void setup() {
        uut = new TopConsumerSummary(NAME, VALUE);
    }

    @Test
    public void testBuildSummaryMessage() {
        TopConsumerSummaryMessage msg = uut.buildSummaryMessage();
        Assert.assertEquals(NAME, msg.getName());
        Assert.assertEquals(VALUE, msg.getValue(), 0);
    }

    @Test
    public void testBuildSummaryMessageAndAddToFlowUnit() {
        // No assertions need to be made here, this function is a noop in the uut
        FlowUnitMessage.Builder msgBuilder = FlowUnitMessage.newBuilder();
        uut.buildSummaryMessageAndAddToFlowUnit(msgBuilder);
    }

    @Test
    public void testBuildTopConsumerSummaryFromMessage() {
        TopConsumerSummary summary = TopConsumerSummary.buildTopConsumerSummaryFromMessage(uut.buildSummaryMessage());
        Assert.assertEquals(uut.getName(), summary.getName());
        Assert.assertEquals(uut.getValue(), summary.getValue(), 0);
    }

    @Test
    public void testToString() {
        Assert.assertEquals(NAME + " " + VALUE, uut.toString());
    }

    @Test
    public void testGetTableName() {
        Assert.assertEquals(TopConsumerSummary.TOP_CONSUMER_SUMMARY_TABLE, uut.getTableName());
    }

    @Test
    public void testGetSqlSchema() {
        List<Field<?>> schema = uut.getSqlSchema();
        Assert.assertEquals(2, schema.size());
        Field<?> nameField = schema.get(0);
        Field<?> valueField = schema.get(1);
        Assert.assertEquals(TopConsumerSummary.SQL_SCHEMA_CONSTANTS.CONSUMER_NAME_COL_NAME, nameField.getName());
        Assert.assertEquals(String.class, nameField.getType());
        Assert.assertEquals(TopConsumerSummary.SQL_SCHEMA_CONSTANTS.CONSUMER_VALUE_COL_NAME, valueField.getName());
        Assert.assertEquals(Double.class, valueField.getType());
    }

    @Test
    public void testGetSqlValue() {
        List<Object> rows = uut.getSqlValue();
        Assert.assertEquals(2, rows.size());
        Assert.assertEquals(NAME, rows.get(0));
        Assert.assertEquals(VALUE, rows.get(1));
    }

    @Test
    public void testToJson() {
        Assert.assertNull(uut.toJson());
    }
}
