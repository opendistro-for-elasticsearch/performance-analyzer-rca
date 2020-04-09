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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.contexts;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Resources;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.GenericContext;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class GenericContextTest {
    private GenericContext uut;

    @Before
    public void setup() {
        uut = new GenericContext(Resources.State.HEALTHY);
    }

    @Test
    public void testToString() {
        Assert.assertEquals(Resources.State.HEALTHY, uut.getState());
        Assert.assertEquals(Resources.State.HEALTHY.toString(), uut.toString());
    }

    @Test
    public void testIsUnknown() {
        Assert.assertFalse(uut.isUnknown());
        Assert.assertTrue(GenericContext.generic().isUnknown());
    }

    @Test
    public void testIsUnhealthy() {
        Assert.assertFalse(ResourceContext.generic().isUnhealthy());
        Assert.assertFalse(uut.isUnhealthy());
        uut = new ResourceContext(Resources.State.CONTENDED);
        Assert.assertTrue(uut.isUnhealthy());
        uut = new ResourceContext(Resources.State.UNHEALTHY);
        Assert.assertTrue(uut.isUnhealthy());
    }
}
