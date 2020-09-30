package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.jvmsizing;

import static com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.GCType.OLD_GEN;
import static com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.GCType.TOT_FULL_GC;
import static com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.HeapDimension.MEM_TYPE;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.metricsdb.MetricsDB;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Metric;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Rca;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Resources.State;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.aggregators.SlidingWindow;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.aggregators.SlidingWindowData;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.contexts.ResourceContext;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.flow_units.MetricFlowUnit;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.flow_units.ResourceFlowUnit;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.persist.SQLParsingUtil;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotResourceSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.ResourceUtil;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.scheduler.FlowUnitOperationArgWrapper;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.hotheap.HighHeapUsageOldGenRca.MinOldGenSlidingWindow;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class OldGenReclamationRca extends Rca<ResourceFlowUnit<HotResourceSummary>> {

  private static final long EVAL_INTERVAL_IN_S = 5;
  private static final double DEFAULT_TARGET_UTILIZATION_AFTER_GC = 75.0d;
  private static final long DEFAULT_RCA_EVALUATION_INTERVAL_IN_S = 60;
  private static final long B_TO_MB = 1024 * 1024;

  private final MinOldGenSlidingWindow minOldGenSlidingWindow;
  private final SlidingWindow<SlidingWindowData> gcEventsSlidingWindow;

  private Metric heapUsed;
  private Metric gcEvent;
  private Metric heapMax;
  private HotResourceSummary prevSummary;
  private ResourceContext prevContext;
  private double targetHeapUtilizationAfterGc;
  private long rcaEvaluationIntervalInS;
  private long rcaPeriod;
  private int samples;

  public OldGenReclamationRca(final Metric heapUsed, final Metric heapMax, final Metric gcEvent) {
    this(heapUsed, heapMax, gcEvent, DEFAULT_TARGET_UTILIZATION_AFTER_GC,
        DEFAULT_RCA_EVALUATION_INTERVAL_IN_S);
  }

  public OldGenReclamationRca(final Metric heapUsed, final Metric heapMax, final Metric gcEvent,
      final double targetHeapUtilizationAfterGc, final long rcaEvaluationIntervalInS) {
    super(EVAL_INTERVAL_IN_S);
    this.heapUsed = heapUsed;
    this.gcEvent = gcEvent;
    this.heapMax = heapMax;
    this.targetHeapUtilizationAfterGc = targetHeapUtilizationAfterGc;
    this.rcaEvaluationIntervalInS = rcaEvaluationIntervalInS;
    this.rcaPeriod = rcaEvaluationIntervalInS / EVAL_INTERVAL_IN_S;
    this.samples = 0;
    this.minOldGenSlidingWindow = new MinOldGenSlidingWindow(1, TimeUnit.MINUTES);
    this.gcEventsSlidingWindow = new SlidingWindow<>(1, TimeUnit.MINUTES);
    this.prevContext = new ResourceContext(State.UNKNOWN);
    this.prevSummary = null;
  }

  @Override
  public void generateFlowUnitListFromWire(FlowUnitOperationArgWrapper args) {
    throw new UnsupportedOperationException("generateFlowUnitListFromWire should not be called "
        + "for node-local rca: " + args.getNode().name());
  }

  @Override
  public ResourceFlowUnit<HotResourceSummary> operate() {
    samples++;
    double oldGenMax = getOldGenValueForMetric(heapMax);
    double oldGenUsed = getOldGenValueForMetric(heapUsed);
    double gcEvents = getGcEvents();
    long currTime = System.currentTimeMillis();
    minOldGenSlidingWindow.next(new SlidingWindowData(currTime, oldGenUsed));
    gcEventsSlidingWindow.next(new SlidingWindowData(currTime, gcEvents));

    if (samples == rcaPeriod) {
      samples = 0;
      double events = gcEventsSlidingWindow.readSum();
      if (events >= 1) {
        double threshold = targetHeapUtilizationAfterGc / 100d * oldGenMax;
        HotResourceSummary summary = null;
        ResourceContext context = null;
        if (minOldGenSlidingWindow.readMin() > threshold) {
          summary = new HotResourceSummary(ResourceUtil.FULL_GC_EFFECTIVENESS,
              targetHeapUtilizationAfterGc, minOldGenSlidingWindow.readMin(), 60);
          context = new ResourceContext(State.UNHEALTHY);

          return new ResourceFlowUnit<>(currTime, context, summary);
        } else {
          summary = new HotResourceSummary(ResourceUtil.FULL_GC_EFFECTIVENESS,
              targetHeapUtilizationAfterGc, minOldGenSlidingWindow.readMin(), 60);
          context = new ResourceContext(State.HEALTHY);
        }

        prevSummary = summary;
        prevContext = context;

        return new ResourceFlowUnit<>(currTime, context, summary);
      }
    }

    return new ResourceFlowUnit<>(currTime, prevContext, prevSummary);
  }

  private double getGcEvents() {
    List<MetricFlowUnit> gcEventMetricFlowUnits = gcEvent.getFlowUnits();
    double metricValue = 0d;
    for (final MetricFlowUnit gcEventMetricFlowUnit : gcEventMetricFlowUnits) {
      if (gcEventMetricFlowUnit.isEmpty()) {
        continue;
      }

      double ret = SQLParsingUtil.readDataFromSqlResult(gcEventMetricFlowUnit.getData(),
          MEM_TYPE.getField(),
          TOT_FULL_GC.toString(), MetricsDB.MAX);
      if (!Double.isNaN(ret)) {
        metricValue = ret;
      }
    }

    return metricValue;
  }

  private double getOldGenValueForMetric(Metric heapMetric) {
    List<MetricFlowUnit> heapMetricFlowUnits = heapMetric.getFlowUnits();
    double metricValue = 0d;
    for (final MetricFlowUnit heapMetricFlowUnit : heapMetricFlowUnits) {
      if (heapMetricFlowUnit.isEmpty()) {
        continue;
      }

      double ret = SQLParsingUtil.readDataFromSqlResult(heapMetricFlowUnit.getData(),
          MEM_TYPE.getField(),
          OLD_GEN.toString(), MetricsDB.MAX);
      if (!Double.isNaN(ret)) {
        metricValue = ret / B_TO_MB;
      }
    }

    return metricValue;
  }
}
