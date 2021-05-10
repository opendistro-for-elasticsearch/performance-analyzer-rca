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

package com.amazon.opendistro.elasticsearch.performanceanalyzer;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.collectors.StatsCollector;
import org.junit.Assert;
import org.junit.Test;

public class PerformanceAnalyzerAppTest {

  @Test
  public void testMain() {
    PerformanceAnalyzerApp.main(new String[0]);
    Assert.assertFalse(ConfigStatus.INSTANCE.haveValidConfig());
    Assert.assertEquals(StatsCollector.instance().getCounters().get("ReaderThreadStopped").get(), 1);
    Assert.assertEquals(StatsCollector.instance().getCounters().get("TotalError").get(), 1);
    Assert.assertFalse(StatsCollector.instance().getCounters().containsKey("ReaderRestartProcessing"));
  }
}
