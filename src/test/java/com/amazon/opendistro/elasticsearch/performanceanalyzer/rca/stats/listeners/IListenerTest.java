/*
 *  Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License").
 *  You may not use this file except in compliance with the License.
 *  A copy of the License is located at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  or in the "license" file accompanying this file. This file is distributed
 *  on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *  express or implied. See the License for the specific language governing
 *  permissions and limitations under the License.
 */

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.stats.listeners;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.stats.collectors.SampleAggregator;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.stats.measurements.MeasurementSet;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.stats.measurements.MeasurementSetTestHelper;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Assert;
import org.junit.Test;

public class IListenerTest {
  class Listener implements IListener {
    private AtomicInteger count;

    public Listener() {
      count = new AtomicInteger(0);
    }

    @Override
    public Set<MeasurementSet> getMeasurementsListenedTo() {
      Set<MeasurementSet> set =
          new HashSet() {
            {
              this.add(MeasurementSetTestHelper.TEST_MEASUREMENT1);
              this.add(MeasurementSetTestHelper.TEST_MEASUREMENT2);
            }
          };
      return set;
    }

    @Override
    public void onOccurrence(MeasurementSet measurementSet, Number value, String key) {
      count.getAndIncrement();
    }
  }

  @Test
  public void onOccurrence() {
    Listener listener = new Listener();
    SampleAggregator sampleAggregator =
        new SampleAggregator(
            listener.getMeasurementsListenedTo(), listener, MeasurementSetTestHelper.values());

    sampleAggregator.updateStat(MeasurementSetTestHelper.TEST_MEASUREMENT4, "", 1);
    sampleAggregator.updateStat(MeasurementSetTestHelper.TEST_MEASUREMENT1, "", 1);
    sampleAggregator.updateStat(MeasurementSetTestHelper.TEST_MEASUREMENT2, "", 1);

    Assert.assertEquals(2, listener.count.get());
  }
}
