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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.jvm;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.OSMetricsGeneratorFactory;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.collectors.StatExceptionCode;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.collectors.StatsCollector;
import java.lang.management.ThreadInfo;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

// This test only runs in linux systems as the some of the static members of the ThreadList
// class are specific to Linux.
public class ThreadListTest {
    @Before
    public void before() {
        org.junit.Assume.assumeNotNull(OSMetricsGeneratorFactory.getInstance());
    }

    @Test
    public void testNullThreadInfo() {
        String propertyName = "clk.tck";
        String old_clk_tck = System.getProperty(propertyName);
        System.setProperty(propertyName, "100");
        ThreadInfo[] infos = ThreadList.getAllThreadInfos();
        // Artificially injecting a null to simulate that the thread id does not exist
        // any more and therefore the corresponding threadInfo is null.
        infos[0] = null;

        ThreadList.parseAllThreadInfos(infos);

        Map<String, AtomicInteger> counters = StatsCollector.instance().getCounters();

        Assert.assertEquals(counters.get(StatExceptionCode.JVM_THREAD_ID_NO_LONGER_EXISTS.toString()).get(), 1);

        if (old_clk_tck != null) {
            System.setProperty(propertyName, old_clk_tck);
        }
    }
}
