package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.cluster;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Rca;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.flow_units.ResourceFlowUnit;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotNodeSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.StaticBucketThresholds;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.UsageBucketThresholds;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.RcaConf;
import com.google.common.collect.Lists;
import java.util.List;

public class HighCpuClusterRca extends BaseClusterRca {
  public static final String RCA_TABLE_NAME = HighCpuClusterRca.class.getSimpleName();

  private List<Double> cpuUsageThresholds = Lists.newArrayList(20.0, 40.0, 75.0);


  @SafeVarargs
  public <R extends Rca<ResourceFlowUnit<HotNodeSummary>>> HighCpuClusterRca(
      int rcaPeriod, R... nodeRca) {
    super(rcaPeriod, nodeRca);
    this.computeUsageBuckets = true;
    this.collectFromMasterNode = true;
  }

  @Override
  public void readRcaConf(RcaConf conf) {
    cpuUsageThresholds = conf.getUsageBucketThresholds(UsageBucketThresholds.CPU_USAGE);
  }

  @Override
  protected UsageBucketThresholds getBucketThresholds() {
    return new StaticBucketThresholds(cpuUsageThresholds.get(0), cpuUsageThresholds.get(1),
        cpuUsageThresholds.get(2));
  }
}
