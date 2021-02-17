/*
 * Copyright 2021 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.samplers;

import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.AppContext;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.RcaController;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class RcaStateSamplersTest {
    private RcaStateSamplers uut;

    @Test
    public void testGetRcaEnabledSampler() {  // done for constructor coverage
        uut = new RcaStateSamplers();
        assertSame(uut.getClass(), RcaStateSamplers.class);
        assertTrue(RcaStateSamplers.getRcaEnabledSampler(new AppContext()) instanceof RcaEnabledSampler);
    }
}
