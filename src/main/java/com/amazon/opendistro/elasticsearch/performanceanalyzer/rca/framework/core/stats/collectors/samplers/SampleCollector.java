package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.stats.collectors.samplers;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.stats.collectors.Collector;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.stats.eval.Statistics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.stats.eval.impl.Sample;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.stats.eval.impl.vals.Value;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.stats.format.Formatter;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.stats.measurements.MeasurementSet;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.stats.measurements.sampled.SampledMeasurements;
import com.google.common.collect.ImmutableMap;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class SampleCollector implements Collector {
  private static final Logger LOG = LogManager.getLogger(SampleCollector.class);
  /** These are the measurement types this collector keeps track of. */
  private final SampledMeasurements[] sampleSet;

  /** When was the first updateStat was called since the last reset. */
  private AtomicLong startTimeMillis;

  /** This map keeps track of the samples corresponding to each measurement type. */
  private ImmutableMap<SampledMeasurements, Sample> measurementSampleValueMap;

  public SampleCollector(SampledMeasurements[] sampleSet) {
    this.sampleSet = sampleSet;
    init();
  }

  private void init() {
    startTimeMillis = new AtomicLong(0L);
    Map<SampledMeasurements, Sample> tempMap = new HashMap<>();

    for (SampledMeasurements measurement : sampleSet) {
      tempMap.put(measurement, new Sample());
    }
    this.measurementSampleValueMap = ImmutableMap.copyOf(tempMap);
  }

  public void updateSample(MeasurementSet metric, Number value) {
    Sample sample = measurementSampleValueMap.get(metric);
    if (sample == null) {
      LOG.error("'{}' asked to be aggregated, when known types are only: {}", metric, sampleSet);
      return;
    }

    if (startTimeMillis.get() == 0L) {
      // The CAS operations are expensive compared to primitive type checks. Therefore, we only
      // resort to CAS if we even stand a chance of modifying the variable. The startTime is only
      // set by the first thread that tries to update a metric. So, we don't want all the
      // subsequent threads to pay the price of a CAS.
      startTimeMillis.compareAndSet(0L, System.currentTimeMillis());
    }

    sample.calculate("", value);
  }

  @Override
  public void fillValuesAndReset(Formatter formatter) {
    synchronized (this) {
      fill(formatter);
      init();
    }
  }

  private void fill(Formatter formatter) {
    long endTime = System.currentTimeMillis();
    formatter.setStartAndEndTime(startTimeMillis.get(), endTime);

    for (Map.Entry<SampledMeasurements, Sample> entry : measurementSampleValueMap.entrySet()) {
      Sample sample = entry.getValue();
      if (!sample.empty()) {
        Collection<Value> values = sample.get();
        for (Value value : values) {
          value.format(formatter, entry.getKey(), Statistics.SAMPLE);
        }
      }
    }
  }
}
