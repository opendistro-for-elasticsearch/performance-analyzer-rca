package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.stats.collectors.aggregator;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.stats.eval.Statistics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.stats.eval.impl.Count;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.stats.eval.impl.Max;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.stats.eval.impl.Mean;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.stats.eval.impl.Min;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.stats.eval.impl.NamedCounter;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.stats.eval.impl.Sample;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.stats.eval.impl.StatisticImpl;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.stats.eval.impl.Sum;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.stats.eval.impl.vals.Value;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.stats.format.Formatter;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.stats.measurements.MeasurementSet;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * This class is mainly to collect stats between runs of the RCA framework before we can write them
 * using the Stats Collector.
 *
 * <p>This is suitable for cases where we want to calculate statistics before we report it, e.g the
 * RCA graph evaluation. We want to know the long pole in the Graph node execution and how much it
 * deviates from the mean but we also don't want to report the time taken by each graph node.
 */
public class SampleAggregator {

  private static final Logger LOG = LogManager.getLogger(SampleAggregator.class);
  /**
   * The idea is to be able to calculate multiple statistics for each measurement.
   *
   * <ul>
   *   <li>key: Measurement are anything that we want to sample, say graphNodeExecution.
   *   <li>value: The list of objects that calculates various metrics, say an object implementing
   *       mean and another one implementing Max.
   * </ul>
   */
  private ImmutableMap<MeasurementSet, List<StatisticImpl>> statMap;

  /** When was the first updateStat was called since the last reset. */
  private AtomicLong startTimeMillis;

  /** The set of measurements its in charge of aggregating. */
  private MeasurementSet[] recognizedSet;

  public SampleAggregator(MeasurementSet[] measurementSet) {
    this.recognizedSet = measurementSet;
    init();
  }

  private void init() {
    startTimeMillis = new AtomicLong(0L);
    Map<MeasurementSet, List<StatisticImpl>> initializer = new ConcurrentHashMap<>();

    for (MeasurementSet elem : recognizedSet) {
      List<StatisticImpl> impls = new ArrayList<>();
      for (Statistics stats : elem.getStatsList()) {
        switch (stats) {
          case COUNT:
            impls.add(new Count());
            break;
          case MAX:
            impls.add(new Max());
            break;
          case MEAN:
            impls.add(new Mean());
            break;
          case MIN:
            impls.add(new Min());
            break;
          case NAMED_COUNTERS:
            impls.add(new NamedCounter());
            break;
          case SAMPLE:
            impls.add(new Sample());
            break;
          case SUM:
            impls.add(new Sum());
            break;
          default:
            throw new IllegalArgumentException("Unimplemented stat: " + stats);
        }
      }
      initializer.put(elem, impls);
    }
    this.statMap = ImmutableMap.copyOf(initializer);
  }

  /**
   * This is called whenever the framework hits a measurement of interest. This is thread safe.
   *
   * @param metric Determined by the Enum MeasurementType
   * @param key multiple points in the code can emit the same measurement, say RCA1 and RCA2, both
   *     will emit a measurement how long each of them took and then this metric will determine
   *     which of the two took the longest(Max).
   * @param value The actual value of the measurement.
   * @param <V> The Type of value
   */
  public <V extends Number> void updateStat(MeasurementSet metric, String key, V value) {
    List<StatisticImpl> statistics = statMap.get(metric);
    if (statistics == null) {
      LOG.error(
          "'{}' asked to be aggregated, when known types are only: {}", metric, recognizedSet);
      return;
    }

    if (startTimeMillis.get() == 0L) {
      // The CAS operations are expensive compared to primitive type checks. Therefore, we only
      // resort to CAS if we even stand a chance of modifying the variable. The startTime is only
      // set by the first thread that tries to update a metric. So, we don't want all the
      // subsequent threads to pay the price of a CAS.
      startTimeMillis.compareAndSet(0L, System.currentTimeMillis());
    }

    for (StatisticImpl s : statistics) {
      s.calculate(key, value);
    }
  }

  /**
   * This gets the current set of Measurements collected and re-initiates the objects for the next
   * iteration.
   *
   * @param formatter An class that knows how to format a map of enum and lists.
   */
  public void fillValuesAndReset(Formatter formatter) {
    synchronized (this) {
      fill(formatter);
      init();
    }
  }

  /**
   * Be advised that the statMap is filled in just once in the constructor. Ever since no new
   * elements are added just existing elements are modified. Therefore, some of the statistics that
   * have already been added at initialization might not ever be calculated, if <code>updateStat()
   * </code> is never called on it. Therefore, it such values are not desired, then the same can be
   * checked using the <code>calculatedAtLeastOnce()</code> flag.
   *
   * @param formatter Used to convert the map into a desired format.
   */
  public void fill(Formatter formatter) {
    long endTime = System.currentTimeMillis();
    formatter.setStartAndEndTime(startTimeMillis.get(), endTime);

    for (Map.Entry<MeasurementSet, List<StatisticImpl>> entry : statMap.entrySet()) {
      MeasurementSet measurement = entry.getKey();
      for (StatisticImpl statValues : entry.getValue()) {
        if (!statValues.isEmpty()) {
          Statistics stat = statValues.type();
          Collection<Value> values = statValues.get();
          for (Value value : values) {
            value.format(formatter, measurement, stat);
          }
        }
      }
    }
  }

  @VisibleForTesting
  public boolean isMeasurementObserved(MeasurementSet toFind) {
    List<StatisticImpl> statistics = statMap.get(toFind);
    if (statistics == null) {
      return false;
    }
    for (StatisticImpl statistic : statMap.get(toFind)) {
      if (statistic != null && !statistic.isEmpty()) {
        return true;
      }
    }
    return false;
  }
}
