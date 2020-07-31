package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotResourceSummary.UsageBucket;

public interface UsageBucketThresholds {
  String YOUNG_GEN_PROMOTION_RATE = "young-gen-heap-promotion-rate";
  String OLD_GEN_HEAP_USAGE = "old-gen-heap-usage";
  String CPU_USAGE = "cpu-usage";

  public UsageBucket computeBucket(HotResourceSummary hotResourceSummary);
}
