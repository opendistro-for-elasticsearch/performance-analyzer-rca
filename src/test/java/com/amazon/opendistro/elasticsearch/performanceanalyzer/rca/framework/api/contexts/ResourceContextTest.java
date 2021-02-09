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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.contexts;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.ResourceContextMessage;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Resources;
import java.util.List;
import org.jooq.Field;
import org.jooq.impl.DSL;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;


public class ResourceContextTest {
    private ResourceContext uut;

    @Before
    public void setup() {
        uut = new ResourceContext(Resources.State.HEALTHY);
    }

    @Test
    public void testBuildContextMessage() {
        ResourceContextMessage msg = uut.buildContextMessage();
        Assert.assertEquals(Resources.State.HEALTHY.ordinal(), msg.getState());
    }

    @Test
    public void testBuildResourceContextFromMessage() {
        ResourceContextMessage msg = ResourceContextMessage.newBuilder().setState(Resources.State.HEALTHY.ordinal())
                .build();
        ResourceContext context = ResourceContext.buildResourceContextFromMessage(msg);
        Assert.assertEquals(Resources.State.HEALTHY, context.getState());
    }

    @Test
    public void testIsUnknown() {
        Assert.assertFalse(uut.isUnknown());
        Assert.assertTrue(ResourceContext.generic().isUnknown());
    }

    @Test
    public void testGetSqlSchema() {
        List<Field<?>> schema = uut.getSqlSchema();
        // Done for static class coverage
        Assert.assertTrue(new ResourceContext.SQL_SCHEMA_CONSTANTS() instanceof ResourceContext.SQL_SCHEMA_CONSTANTS);
        Assert.assertEquals(1, schema.size());
        Assert.assertEquals("State", ResourceContext.SQL_SCHEMA_CONSTANTS.STATE_COL_NAME);
        Assert.assertEquals(DSL.field(DSL.name(ResourceContext.SQL_SCHEMA_CONSTANTS.STATE_COL_NAME), String.class),
                schema.get(0));
    }

    @Test
    public void testGetSqlValue() {
        List<Object> values = uut.getSqlValue();
        Assert.assertEquals(1, values.size());
        Assert.assertEquals(Resources.State.HEALTHY.toString(), values.get(0));
    }
}
