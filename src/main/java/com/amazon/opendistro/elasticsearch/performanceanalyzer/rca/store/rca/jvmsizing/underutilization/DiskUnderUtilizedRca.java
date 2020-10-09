package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.jvmsizing.underutilization;

import static com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.util.RcaConsts.DISK_UU_RCA;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.collectors.StatsCollector;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.core.Util;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.DevicePartitionDimension;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metricsdb.MetricsDB;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.configs.DiskUnderUtilizedRcaConfig;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Metric;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Rca;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Resources.State;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.contexts.ResourceContext;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.flow_units.MetricFlowUnit;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.flow_units.ResourceFlowUnit;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.persist.SQLParsingUtil;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotResourceSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.ResourceUtil;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.RcaConf;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.scheduler.FlowUnitOperationArgWrapper;
import java.io.IOException;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.Record;
import org.jooq.Result;

public class DiskUnderUtilizedRca extends Rca<ResourceFlowUnit<HotResourceSummary>> {

  private static final Logger LOG = LogManager.getLogger(DiskUnderUtilizedRca.class);
  private static final long EVAL_INTERVAL_IN_S = 5;
  private final Metric totalSpaceMetric;
  private final Metric shardSizeInBytes;
  private double shardSpaceUtilizationThreshold;

  public DiskUnderUtilizedRca(final Metric totalSpaceMetric, final Metric shardSizeInBytes) {
    this(totalSpaceMetric, shardSizeInBytes,
        DiskUnderUtilizedRcaConfig.DEFAULT_SHARD_DISK_SPACE_UTILIZATION_PERCENT);
  }

  public DiskUnderUtilizedRca(final Metric totalSpaceMetric, final Metric shardSizeInBytes,
      final double shardSpaceUtilizationThreshold) {
    super(EVAL_INTERVAL_IN_S);
    this.totalSpaceMetric = totalSpaceMetric;
    this.shardSizeInBytes = shardSizeInBytes;
    this.shardSpaceUtilizationThreshold = shardSpaceUtilizationThreshold;
  }

  @Override
  public void generateFlowUnitListFromWire(FlowUnitOperationArgWrapper args) {
    throw new UnsupportedOperationException("generateFlowUnitListFromWire should not be called "
        + "for node-local rca: " + args.getNode().name());
  }

  @Override
  public ResourceFlowUnit<HotResourceSummary> operate() {
    long currTime = System.currentTimeMillis();
    double totalPartitionSpace = getTotalPartitionSpace();
    double totalShardSize = getTotalShardSize();
    double diskSpaceUsed = (totalShardSize / totalPartitionSpace) * 100D;

    if (diskSpaceUsed < shardSpaceUtilizationThreshold) {
      // cumulative shard size on disk is less than the threshold. So, classify this as
      // underutilized disk. We also don't need to maintain a sliding window as shard size on
      // disk is not a metric to vary(decrease - increase - decrease) by huge margins minute to
      // minute to affect the under-utilization calculation. We can treat the current snapshot to
      // be the representative value for the whole of the evaluation period of the node level rca.

      ResourceContext context = new ResourceContext(State.UNDERUTILIZED);
      HotResourceSummary summary = new HotResourceSummary(ResourceUtil.CPU_USAGE,
          shardSpaceUtilizationThreshold, diskSpaceUsed, (int) EVAL_INTERVAL_IN_S);

      return new ResourceFlowUnit<>(currTime, context, summary);
    }

    return new ResourceFlowUnit<>(currTime, new ResourceContext(State.HEALTHY),
        new HotResourceSummary(ResourceUtil.CPU_USAGE, shardSpaceUtilizationThreshold,
            diskSpaceUsed, (int) EVAL_INTERVAL_IN_S));
  }

  private double getTotalShardSize() {
    final List<MetricFlowUnit> metricFlowUnits = shardSizeInBytes.getFlowUnits();
    double totalSpaceUsedByShards = 0;
    for (MetricFlowUnit metricFlowUnit : metricFlowUnits) {
      if (metricFlowUnit.isEmpty()) {
        continue;
      }

      Result<Record> records = metricFlowUnit.getData();
      for (final Record record : records) {
        try {
          Double usage = record.getValue(MetricsDB.MAX, Double.class);
          if (!Double.isNaN(usage)) {
            totalSpaceUsedByShards += usage;
          }
        } catch (Exception e) {
          StatsCollector.instance().logMetric(DISK_UU_RCA);
          LOG.error("Failed to parse metric in FlowUnit: {} from {}", record,
              shardSizeInBytes.name(), e);
        }
      }
    }

    return totalSpaceUsedByShards;
  }

  private double getTotalPartitionSpace() {
    final Path dataDirPath = Paths.get(Util.DATA_DIR);
    if (Files.exists(dataDirPath)) {
      // traverse up the file system to get to the mount point.
      try {
        FileStore fileStore = Files.getFileStore(dataDirPath);
        String partition = fileStore.name();
        double partitionSizeInBytes = 0d;
        final List<MetricFlowUnit> metricFlowUnits = totalSpaceMetric.getFlowUnits();
        for (final MetricFlowUnit metricFlowUnit : metricFlowUnits) {
          if (metricFlowUnit.isEmpty()) {
            continue;
          }

          partitionSizeInBytes = SQLParsingUtil.readDataFromSqlResult(metricFlowUnit.getData(),
              DevicePartitionDimension.DEVICE_PARTITION.getField(), partition, MetricsDB.MAX);

          if (!Double.isNaN(partitionSizeInBytes) && partitionSizeInBytes > 0) {
            break;
          }
        }
        return partitionSizeInBytes;
      } catch (IOException e) {
        StatsCollector.instance().logMetric(DISK_UU_RCA);
        LOG.error("Couldn't get the FileStore for the for data directory: {}",
            dataDirPath.toString(), e);
        return Double.NaN;
      }
    } else {
      throw new IllegalStateException("Data directory path specified: " + Util.DATA_DIR
          + " is not a valid file or a directory.");
    }
  }

  @Override
  public void readRcaConf(RcaConf conf) {
    final DiskUnderUtilizedRcaConfig config = conf.getDiskUnderUtilizedRcaConfig();
    this.shardSpaceUtilizationThreshold = config.getShardDiskSpaceUtilizationThreshold();
  }
}
