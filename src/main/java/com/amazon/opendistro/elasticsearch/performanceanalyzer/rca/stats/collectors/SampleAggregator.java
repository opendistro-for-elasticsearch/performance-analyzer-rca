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

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.stats.eval.Statistics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.stats.eval.impl.Count;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.stats.eval.impl.IStatistic;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.stats.eval.impl.Max;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.stats.eval.impl.Mean;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.stats.eval.impl.Min;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.stats.eval.impl.NamedCounter;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.stats.eval.impl.Sample;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.stats.eval.impl.Sum;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.stats.eval.impl.vals.Value;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.stats.format.Formatter;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.stats.listeners.IListener;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.stats.measurements.MeasurementSet;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
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
  /** The set of measurements its in charge of aggregating. */
  private final MeasurementSet[] recognizedSet;
  /**
   * The idea is to be able to calculate multiple statistics for each measurement.
   *
   * <ul>
   *   <li>key: Measurement are anything that we want to sample, say graphNodeExecution.
   *   <li>value: The list of objects that calculates various metrics, say an object implementing
   *       mean and another one implementing Max.
   * </ul>
   */
  private ImmutableMap<MeasurementSet, Set<IStatistic>> statMap;
  /** When was the first updateStat was called since the last reset. */
  private AtomicLong startTimeMillis;

  /** Listeners for the occurrence of a metric being emitted. */
  private final IListener listener;

  /** List of measurements being listened to. */
  private final Set<MeasurementSet> listenedMeasurements;

  public SampleAggregator(MeasurementSet[] measurementSet) {
    this(Collections.EMPTY_SET, null, measurementSet);
  }

  public SampleAggregator(
      final Set<MeasurementSet> listenedMeasurements,
      final IListener listener,
      final MeasurementSet[] measurementSet) {
    Objects.requireNonNull(listenedMeasurements);
    this.listenedMeasurements = listenedMeasurements;
    this.listener = listener;
    this.recognizedSet = measurementSet;
    init();
  }

  private void init() {
    startTimeMillis = new AtomicLong(0L);
    Map<MeasurementSet, Set<IStatistic>> initializer = new ConcurrentHashMap<>();

    for (MeasurementSet elem : recognizedSet) {
      Set<IStatistic> impls = new HashSet<>();
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
    Set<IStatistic> statistics = statMap.get(metric);
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

    for (IStatistic s : statistics) {
      s.calculate(key, value);
    }

    if (listenedMeasurements.contains(metric)) {
      listener.onOccurrence(metric, value, key);
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

    for (Map.Entry<MeasurementSet, Set<IStatistic>> entry : statMap.entrySet()) {
      MeasurementSet measurement = entry.getKey();
      for (IStatistic statValues : entry.getValue()) {
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
    Set<IStatistic> statistics = statMap.get(toFind);
    if (statistics == null) {
      return false;
    }
    for (IStatistic statistic : statMap.get(toFind)) {
      if (statistic != null && !statistic.isEmpty()) {
        return true;
      }
    }
    return false;
  }

  @VisibleForTesting
  public Collection<IStatistic> getValues(MeasurementSet toFind) {
    Set<IStatistic> statistics = statMap.get(toFind);
    if (statistics == null) {
      return Collections.EMPTY_LIST;
    }
    return statMap.get(toFind);
  }
}
