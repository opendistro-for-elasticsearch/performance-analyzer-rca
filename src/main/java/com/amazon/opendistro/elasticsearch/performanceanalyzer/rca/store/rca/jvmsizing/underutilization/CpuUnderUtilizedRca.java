package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.jvmsizing.underutilization;

import static com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.util.RcaConsts.CPU_UU_RCA;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.collectors.StatsCollector;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metricsdb.MetricsDB;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.configs.CpuUnderUtilizedRcaConfig;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Metric;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Rca;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Resources.State;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.aggregators.SlidingWindow;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.aggregators.SlidingWindowData;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.contexts.ResourceContext;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.flow_units.MetricFlowUnit;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.flow_units.ResourceFlowUnit;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotResourceSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.ResourceUtil;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.RcaConf;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.scheduler.FlowUnitOperationArgWrapper;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.Record;
import org.jooq.Result;

public class CpuUnderUtilizedRca extends Rca<ResourceFlowUnit<HotResourceSummary>> {

  private static final Logger LOG = LogManager.getLogger(CpuUnderUtilizedRca.class);
  private static final long EVAL_INTERVAL_IN_S = 5;
  private static final long DEFAULT_RCA_PERIOD = 60;
  private static final double DEFAULT_UPPER_BOUND = 25D;
  private final Metric cpuUtilization;
  private final long rcaSamplesBeforeEval;
  private final int numCores;
  private long samples;
  private double cpuUtilizationUpperBound;

  private final SlidingWindow<SlidingWindowData> cpuUtilizationSlidingWindow;

  public CpuUnderUtilizedRca(final Metric cpuUtilization) {
    this(cpuUtilization, DEFAULT_RCA_PERIOD);
  }

  public CpuUnderUtilizedRca(final Metric cpuUtilization, final long rcaEvaluationIntervalInS) {
    super(EVAL_INTERVAL_IN_S);
    this.numCores = Runtime.getRuntime().availableProcessors();
    this.cpuUtilization = cpuUtilization;
    rcaSamplesBeforeEval = rcaEvaluationIntervalInS / EVAL_INTERVAL_IN_S;
    this.cpuUtilizationSlidingWindow = new SlidingWindow<>((int) rcaEvaluationIntervalInS,
        TimeUnit.MINUTES);
    this.samples = 0;
    this.cpuUtilizationUpperBound = DEFAULT_UPPER_BOUND;
  }

  @Override
  public void generateFlowUnitListFromWire(FlowUnitOperationArgWrapper args) {
    throw new UnsupportedOperationException("generateFlowUnitListFromWire should not be called "
        + "for node-local rca: " + args.getNode().name());
  }

  @Override
  public ResourceFlowUnit<HotResourceSummary> operate() {
    samples++;
    addToSlidingWindow();
    if (samples == rcaSamplesBeforeEval) {
      samples = 0;
      return evaluateAndEmit();
    }

    return new ResourceFlowUnit<>(System.currentTimeMillis());
  }

  private void addToSlidingWindow() {
    long currTime = System.currentTimeMillis();
    double totalCpuUtilization = (getTotalCpuUtilization() / numCores) * 100D;

    cpuUtilizationSlidingWindow.next(new SlidingWindowData(currTime, totalCpuUtilization));
  }

  private double getTotalCpuUtilization() {
    List<MetricFlowUnit> metricFlowUnits = cpuUtilization.getFlowUnits();
    double totalUtilization = 0;
    for (final MetricFlowUnit flowUnit : metricFlowUnits) {
      if (flowUnit.isEmpty()) {
        continue;
      }

      Result<Record> records = flowUnit.getData();
      for (final Record record : records) {
        try {
          Double usage = record.getValue(MetricsDB.MAX, Double.class);
          if (!Double.isNaN(usage)) {
            totalUtilization += usage;
          }
        } catch (Exception e) {
          StatsCollector.instance().logMetric(CPU_UU_RCA);
          LOG.error("Filed to parse metric in FlowUnit: {} from {}", record, cpuUtilization.name());
        }
      }
    }

    return totalUtilization;
  }

  private ResourceFlowUnit<HotResourceSummary> evaluateAndEmit() {
    long currTime = System.currentTimeMillis();
    double averageCpuUtilization = cpuUtilizationSlidingWindow.readAvg();
    HotResourceSummary summary = new HotResourceSummary(ResourceUtil.CPU_USAGE,
        cpuUtilizationUpperBound, averageCpuUtilization, (int) rcaSamplesBeforeEval);

    ResourceContext context;
    if (averageCpuUtilization < cpuUtilizationUpperBound) {
      context = new ResourceContext(State.UNDERUTILIZED);
    } else {
      context = new ResourceContext(State.HEALTHY);
    }
    return new ResourceFlowUnit<>(currTime, context, summary);
  }

  @Override
  public void readRcaConf(RcaConf rcaConf) {
    final CpuUnderUtilizedRcaConfig config = rcaConf.getCpuUnderUtilizedRcaConfig();
    this.cpuUtilizationUpperBound = config.getCpuUtilizationThreshold();
  }
}
