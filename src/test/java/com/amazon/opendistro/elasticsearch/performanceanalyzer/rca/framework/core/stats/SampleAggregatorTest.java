package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.stats;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.collectors.StatsCollector;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.GradleTaskForRca;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.stats.collectors.aggregator.SampleAggregator;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.stats.eval.Statistics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.stats.eval.impl.vals.NamedAggregateValue;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.stats.eval.impl.vals.NamedValue;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.stats.eval.impl.vals.Value;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.stats.format.BaseFormatter;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.stats.format.StatsCollectorFormatter;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.stats.measurements.MeasurementSet;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.stats.measurements.aggregated.RcaGraphMeasurements;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(GradleTaskForRca.class)
public class SampleAggregatorTest {

  boolean match(
      Map<MeasurementSet, Map<Statistics, Value>> map,
      long longVal,
      String name,
      double doubleVal,
      boolean matchName) {
    for (Map.Entry<Statistics, Value> s :
        map.get(RcaGraphMeasurements.GRAPH_NODE_OPERATE_CALL).entrySet()) {
      if (s.getKey() == Statistics.MAX) {
        Value v = s.getValue();
        if (longVal != v.getValue().longValue()) {
          System.out.println(longVal + " didn't match " + v.getValue().longValue());
          return false;
        }
        if (matchName) {
          if (v instanceof NamedAggregateValue) {
            String key = ((NamedAggregateValue) v).getName();
            if (!key.equals(name)) {
              System.out.println(key + " didn't match " + name);
              return false;
            }
          } else {
            Assert.fail();
          }
        }
      } else if (s.getKey() == Statistics.MEAN) {
        Value v = s.getValue();
        double c = doubleVal - v.getValue().doubleValue();
        if (Math.abs(c - 1.0) <= 0.001) {
          System.out.println(doubleVal + " didn't match " + v.getValue().doubleValue());
          return false;
        }
      }
    }
    return true;
  }

  @Test
  public void updateStat() {
    SampleAggregator sampleAggregator = new SampleAggregator(RcaGraphMeasurements.values());
    sampleAggregator.updateStat(RcaGraphMeasurements.GRAPH_NODE_OPERATE_CALL, "rca1", 200L);
    sampleAggregator.updateStat(RcaGraphMeasurements.GRAPH_NODE_OPERATE_CALL, "rca2", 400L);
    sampleAggregator.updateStat(RcaGraphMeasurements.GRAPH_NODE_OPERATE_CALL, "rca1", 200L);
    sampleAggregator.updateStat(RcaGraphMeasurements.GRAPH_NODE_OPERATE_CALL, "rca2", 100L);
    sampleAggregator.updateStat(RcaGraphMeasurements.GRAPH_NODE_OPERATE_CALL, "rca0", 200L);
    sampleAggregator.updateStat(RcaGraphMeasurements.GRAPH_NODE_OPERATE_CALL, "rca3", 500L);

    BaseFormatter formatter = new BaseFormatter();
    sampleAggregator.fill(formatter);
    Map<MeasurementSet, Map<Statistics, Value>> map = formatter.getFormatted();

    Assert.assertTrue(match(map, 500, "rca3", 1600.0 / 6, true));

    formatter = new BaseFormatter();
    sampleAggregator.fillValuesAndReset(formatter);
    map = formatter.getFormatted();
    Assert.assertTrue(match(map, 500, "rca3", 1600.0 / 6, true));

    formatter = new BaseFormatter();
    sampleAggregator.fill(formatter);
    map = formatter.getFormatted();
    Assert.assertEquals(0, map.size());
  }

  @Test
  public void updateStatsConcurrent() {

    int N = 10000;
    int T = 1000;

    long[] vals = new long[N];

    long min = Long.MAX_VALUE;
    long max = Long.MIN_VALUE;
    double mean = 0;
    BigInteger sum = BigInteger.ONE;

    Random rand = new Random();
    for (int i = 0; i < N; i++) {
      long x = rand.nextLong();
      vals[i] = x;
      if (x > max) {
        max = x;
      }
      if (x < min) {
        min = x;
      }
      BigInteger old = sum;
      sum = sum.add(BigInteger.valueOf(x));
    }
    mean = sum.doubleValue() / N;

    SampleAggregator sampleAggregator = new SampleAggregator(RcaGraphMeasurements.values());

    // spin off threads

    Assert.assertTrue(N > T);
    Assert.assertEquals(0, N % T);
    int RANGE = N / T;

    List<TestThread> ths = new ArrayList<>();
    for (int i = 0; i < T; i++) {
      int start = RANGE * i;

      ths.add(new TestThread(vals, start, RANGE, sampleAggregator));
    }
    for (TestThread th : ths) {
      th.start();
    }

    for (TestThread th : ths) {
      try {
        th.join();
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }

    BaseFormatter formatter = new BaseFormatter();
    sampleAggregator.fill(formatter);
    Map<MeasurementSet, Map<Statistics, Value>> map = formatter.getFormatted();
    Assert.assertTrue(match(map, max, "", mean, false));
  }

  @Test
  public void dumpStats() {
    Map<String, String> map = new HashMap<>();
    map.put("m1", "v1");
    map.put("m2", "v2");

    StatsCollector statsCollector = new StatsCollector("stats", 1, map);

    SampleAggregator sampleAggregator = new SampleAggregator(RcaGraphMeasurements.values());
    sampleAggregator.updateStat(RcaGraphMeasurements.GRAPH_NODE_OPERATE_CALL, "rca1", 200L);
    sampleAggregator.updateStat(RcaGraphMeasurements.GRAPH_NODE_OPERATE_CALL, "rca2", 400L);
    sampleAggregator.updateStat(RcaGraphMeasurements.GRAPH_NODE_OPERATE_CALL, "rca1", 200L);
    sampleAggregator.updateStat(RcaGraphMeasurements.GRAPH_NODE_OPERATE_CALL, "rca2", 100L);
    sampleAggregator.updateStat(RcaGraphMeasurements.GRAPH_NODE_OPERATE_CALL, "rca0", 200L);
    sampleAggregator.updateStat(RcaGraphMeasurements.GRAPH_NODE_OPERATE_CALL, "rca3", 500L);

    StatsCollectorFormatter formatter = new StatsCollectorFormatter();
    sampleAggregator.fillValuesAndReset(formatter);
    StatsCollectorFormatter.StatsCollectorReturn statsReturn = formatter.getFormatted();
    statsCollector.logStatsRecord(
            statsReturn.getCounters(),
            statsReturn.getStatsdata(),
            statsReturn.getLatencies(),
            statsReturn.getStartTimeMillis(),
            statsReturn.getEndTimeMillis());
  }

  class TestThread extends Thread {
    SampleAggregator sampleAggregator;
    long[] arr;
    private int start;
    private int end;

    TestThread(long[] arr, int idx, int range, SampleAggregator sampleAggregator) {
      this.start = idx;
      this.end = start + range;
      this.sampleAggregator = sampleAggregator;
      this.arr = arr;
    }

    @Override
    public void run() {
      for (int i = start; i < end; i++) {
        sampleAggregator.updateStat(
            RcaGraphMeasurements.GRAPH_NODE_OPERATE_CALL, "th" + start, arr[i]);
      }
    }
  }
}
