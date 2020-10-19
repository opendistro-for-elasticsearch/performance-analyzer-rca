package com.amazon.opendistro.elasticsearch.performanceanalyzer.collectors;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.OSMetricsGeneratorFactory;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.MetricsConfiguration;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.MetricsProcessor;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.PerformanceAnalyzerMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics_generator.MountedPartitionMetricsGenerator;
import java.util.Set;

public class MountedPartitionMetricsCollector extends PerformanceAnalyzerMetricsCollector implements
    MetricsProcessor {

  private static final int SAMPLING_TIME_INTERVAL =
      MetricsConfiguration.CONFIG_MAP.get(MountedPartitionMetricsCollector.class).samplingInterval;
  private static final int EXPECTED_KEYS_PATH_LENGTH = 0;
  private final StringBuilder value;

  public MountedPartitionMetricsCollector() {
    super(SAMPLING_TIME_INTERVAL, "MountedPartition");
    this.value = new StringBuilder();
  }

  @Override
  void collectMetrics(long startTime) {
    MountedPartitionMetricsGenerator mountedPartitionMetricsGenerator =
        OSMetricsGeneratorFactory.getInstance().getMountedPartitionMetricsGenerator();

    mountedPartitionMetricsGenerator.addSample();

    saveMetricValues(getMetrics(mountedPartitionMetricsGenerator), startTime);
  }

  @Override
  public String getMetricsPath(long startTime, String... keysPath) {
    if (keysPath != null && keysPath.length != EXPECTED_KEYS_PATH_LENGTH) {
      throw new RuntimeException("keys length should be " + EXPECTED_KEYS_PATH_LENGTH);
    }

    return PerformanceAnalyzerMetrics.generatePath(startTime,
        PerformanceAnalyzerMetrics.sMountedPartitionMetricsPath);
  }

  private String getMetrics(
      final MountedPartitionMetricsGenerator mountedPartitionMetricsGenerator) {
    // zero the string builder
    value.setLength(0);

    // first line is the timestamp
    value.append(PerformanceAnalyzerMetrics.getJsonCurrentMilliSeconds())
         .append(PerformanceAnalyzerMetrics.sMetricNewLineDelimitor);

    Set<String> mountPoints = mountedPartitionMetricsGenerator.getAllMountPoints();
    for (String mountPoint : mountPoints) {
      value.append(
          new MountedPartitionMetrics(
              mountedPartitionMetricsGenerator.getDevicePartition(mountPoint),
              mountPoint,
              mountedPartitionMetricsGenerator.getTotalSpace(mountPoint),
              mountedPartitionMetricsGenerator.getFreeSpace(mountPoint),
              mountedPartitionMetricsGenerator.getUsableFreeSpace(mountPoint)).serialize())
           .append(PerformanceAnalyzerMetrics.sMetricNewLineDelimitor);
    }
    return value.toString();
  }
}
