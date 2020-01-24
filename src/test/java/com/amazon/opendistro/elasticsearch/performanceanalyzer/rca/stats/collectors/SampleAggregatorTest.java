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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.stats.collectors;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.stats.RcaStatsReporter;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.stats.emitters.ISampler;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.stats.emitters.PeriodicSamplers;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.stats.eval.Statistics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.stats.eval.impl.IStatistic;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.stats.eval.impl.vals.AggregateValue;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.stats.eval.impl.vals.NamedAggregateValue;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.stats.eval.impl.vals.Value;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.stats.measurements.MeasurementSet;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.stats.measurements.MeasurementSetTest;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.junit.Assert;
import org.junit.Test;

public class SampleAggregatorTest {

  private boolean matchList(Collection<Value> l1, Collection<Value> l2) {
    for (Value v1 : l1) {
      boolean matched = false;
      for (Value v2 : l2) {
        if (v1.equals(v2)) {
          matched = true;
          break;
        }
      }
      if (!matched) {
        return false;
      }
    }
    return true;
  }

  private boolean match(
      MeasurementSet measurementSet,
      Map<MeasurementSet, Map<Statistics, List<Value>>> expected,
      SampleAggregator aggregator) {
    for (IStatistic value : aggregator.getValues(measurementSet)) {
      List<Value> expectedValue = expected.get(measurementSet).get(value.type());
      if (!matchList(value.get(), expectedValue)) {
        System.out.println(value.get() + " does not match \n" + expectedValue);
      }
    }
    return true;
  }

  @Test
  public void updateStat() {
    SampleAggregator sampleAggregator = new SampleAggregator(MeasurementSetTest.values());

    ISampler sampler =
        sampleCollector ->
            sampleCollector.updateStat(
                MeasurementSetTest.JVM_FREE_MEM_SAMPLER, "", Runtime.getRuntime().freeMemory());

    PeriodicSamplers periodicSamplers =
        new PeriodicSamplers(
            sampleAggregator, Collections.singletonList(sampler), 10, TimeUnit.HOURS);

    RcaStatsReporter reporter = new RcaStatsReporter(Collections.singletonList(sampleAggregator));

    Map<MeasurementSet, Map<Statistics, List<Value>>> expected = new HashMap<>();

    sampleAggregator.updateStat(MeasurementSetTest.TEST_MEASUREMENT1, "key1", 50L);
    sampleAggregator.updateStat(MeasurementSetTest.TEST_MEASUREMENT1, "key1", 200L);
    sampleAggregator.updateStat(MeasurementSetTest.TEST_MEASUREMENT1, "key1", 200L);
    sampleAggregator.updateStat(MeasurementSetTest.TEST_MEASUREMENT1, "key2", 200L);
    sampleAggregator.updateStat(MeasurementSetTest.TEST_MEASUREMENT1, "key3", 500L);
    sampleAggregator.updateStat(MeasurementSetTest.TEST_MEASUREMENT1, "key1", 200L);
    sampleAggregator.updateStat(MeasurementSetTest.TEST_MEASUREMENT1, "key1", 200L);
    sampleAggregator.updateStat(MeasurementSetTest.TEST_MEASUREMENT1, "key1", 200L);

    expected.put(MeasurementSetTest.TEST_MEASUREMENT1, new HashMap<>());
    expected
        .get(MeasurementSetTest.TEST_MEASUREMENT1)
        .put(
            Statistics.MAX,
            Collections.singletonList(new NamedAggregateValue(500L, Statistics.MAX, "key3")));
    expected
        .get(MeasurementSetTest.TEST_MEASUREMENT1)
        .put(
            Statistics.MIN,
            Collections.singletonList(new NamedAggregateValue(50L, Statistics.MIN, "key1")));
    expected
        .get(MeasurementSetTest.TEST_MEASUREMENT1)
        .put(
            Statistics.MEAN,
            Collections.singletonList(new AggregateValue(1750.0 / 8, Statistics.MEAN)));
    Assert.assertTrue(match(MeasurementSetTest.TEST_MEASUREMENT1, expected, sampleAggregator));

    sampleAggregator.updateStat(MeasurementSetTest.TEST_MEASUREMENT2, "key1", 200L);
    sampleAggregator.updateStat(MeasurementSetTest.TEST_MEASUREMENT2, "key1", 200L);
    sampleAggregator.updateStat(MeasurementSetTest.TEST_MEASUREMENT2, "key1", 200L);
    sampleAggregator.updateStat(MeasurementSetTest.TEST_MEASUREMENT2, "key1", 200L);
    sampleAggregator.updateStat(MeasurementSetTest.TEST_MEASUREMENT2, "key1", 200L);
    sampleAggregator.updateStat(MeasurementSetTest.TEST_MEASUREMENT2, "key1", 200L);

    expected.put(MeasurementSetTest.TEST_MEASUREMENT2, new HashMap<>());
    expected
        .get(MeasurementSetTest.TEST_MEASUREMENT2)
        .put(Statistics.COUNT, Collections.singletonList(new AggregateValue(6, Statistics.COUNT)));
    Assert.assertTrue(match(MeasurementSetTest.TEST_MEASUREMENT2, expected, sampleAggregator));

    sampleAggregator.updateStat(MeasurementSetTest.TEST_MEASUREMENT4, "key1", 200L);
    sampleAggregator.updateStat(MeasurementSetTest.TEST_MEASUREMENT4, "key1", 300L);
    sampleAggregator.updateStat(MeasurementSetTest.TEST_MEASUREMENT4, "key1", 100L);

    expected.put(MeasurementSetTest.TEST_MEASUREMENT4, new HashMap<>());
    expected
        .get(MeasurementSetTest.TEST_MEASUREMENT4)
        .put(Statistics.SAMPLE, Collections.singletonList(new Value(100)));
    Assert.assertTrue(match(MeasurementSetTest.TEST_MEASUREMENT4, expected, sampleAggregator));

    sampleAggregator.updateStat(MeasurementSetTest.TEST_MEASUREMENT5, "key1", 200L);
    sampleAggregator.updateStat(MeasurementSetTest.TEST_MEASUREMENT5, "key1", 300L);
    sampleAggregator.updateStat(MeasurementSetTest.TEST_MEASUREMENT5, "key1", 100L);

    expected.put(MeasurementSetTest.TEST_MEASUREMENT5, new HashMap<>());
    expected
        .get(MeasurementSetTest.TEST_MEASUREMENT5)
        .put(Statistics.SUM, Collections.singletonList(new AggregateValue(600, Statistics.SUM)));
    Assert.assertTrue(match(MeasurementSetTest.TEST_MEASUREMENT5, expected, sampleAggregator));

    sampleAggregator.updateStat(MeasurementSetTest.TEST_MEASUREMENT6, "key1", 200L);
    sampleAggregator.updateStat(MeasurementSetTest.TEST_MEASUREMENT6, "key2", 300L);
    sampleAggregator.updateStat(MeasurementSetTest.TEST_MEASUREMENT6, "key4", 100L);
    sampleAggregator.updateStat(MeasurementSetTest.TEST_MEASUREMENT6, "key1", 200L);
    sampleAggregator.updateStat(MeasurementSetTest.TEST_MEASUREMENT6, "key2", 300L);
    sampleAggregator.updateStat(MeasurementSetTest.TEST_MEASUREMENT6, "key3", 100L);

    expected.put(MeasurementSetTest.TEST_MEASUREMENT6, new HashMap<>());
    expected
        .get(MeasurementSetTest.TEST_MEASUREMENT6)
        .put(
            Statistics.NAMED_COUNTERS,
            Arrays.asList(
                new NamedAggregateValue(2, Statistics.NAMED_COUNTERS, "key1"),
                new NamedAggregateValue(2, Statistics.NAMED_COUNTERS, "key2"),
                new NamedAggregateValue(1, Statistics.NAMED_COUNTERS, "key3"),
                new NamedAggregateValue(1, Statistics.NAMED_COUNTERS, "key4")));
    Assert.assertTrue(match(MeasurementSetTest.TEST_MEASUREMENT6, expected, sampleAggregator));

    sampleAggregator.updateStat(MeasurementSetTest.JVM_FREE_MEM_SAMPLER, "key1", 200L);
    sampleAggregator.updateStat(MeasurementSetTest.JVM_FREE_MEM_SAMPLER, "key2", 500L);

    expected.put(MeasurementSetTest.JVM_FREE_MEM_SAMPLER, new HashMap<>());
    expected
        .get(MeasurementSetTest.JVM_FREE_MEM_SAMPLER)
        .put(Statistics.SAMPLE, Collections.singletonList(new Value(500)));

    Assert.assertTrue(match(MeasurementSetTest.JVM_FREE_MEM_SAMPLER, expected, sampleAggregator));
  }
}
