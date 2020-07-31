package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotResourceSummary.UsageBucket;

public class JvmUsageBucketThresholds implements UsageBucketThresholds {
  private StaticBucketThresholds youngGenPromotionRateThresholds;
  private StaticBucketThresholds oldGenHeapUsageThresholds;

  public JvmUsageBucketThresholds(StaticBucketThresholds youngGenPromotionRateThresholds,
      StaticBucketThresholds oldGenHeapUsageThresholds) {
    this.youngGenPromotionRateThresholds = youngGenPromotionRateThresholds;
    this.oldGenHeapUsageThresholds = oldGenHeapUsageThresholds;
  }

  @Override
  public UsageBucket computeBucket(HotResourceSummary resourceSummary) {
    if (ResourceUtil.getResourceTypeName(resourceSummary.getResource()).equals(
        ResourceUtil.getResourceTypeName(ResourceUtil.YOUNG_GEN_PROMOTION_RATE))) {
      return youngGenPromotionRateThresholds.computeBucket(resourceSummary);
    } else if (ResourceUtil.getResourceTypeName(resourceSummary.getResource()).equals(
        ResourceUtil.getResourceTypeName(ResourceUtil.OLD_GEN_HEAP_USAGE))) {
      return oldGenHeapUsageThresholds.computeBucket(resourceSummary);
    }
    return UsageBucket.UNKNOWN;
  }
}
