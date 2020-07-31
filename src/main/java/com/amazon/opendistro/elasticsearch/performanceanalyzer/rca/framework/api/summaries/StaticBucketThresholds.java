package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotResourceSummary.UsageBucket;

public class StaticBucketThresholds implements UsageBucketThresholds {
  // a value in (-inf, underProvisioned] is considered under provisioned
  double underProvisioned;
  // a value in (underProvisioned, healthyWithBuffer] is considered healthy with
  // a buffer for increasing utilization
  double healthyWithBuffer;
  // a value in (healthyWithBuffer, healthy] is considered healthy, and this implies that a value
  // in (healthy, inf) is considered unhealthy
  double healthy;

  public StaticBucketThresholds() {
    this.underProvisioned = 20.0;
    this.healthyWithBuffer = 40.0;
    this.healthy = 75.0;
  }

  public StaticBucketThresholds(double underProvisioned, double healthyWithBuffer, double healthy) {
    this.underProvisioned = underProvisioned;
    this.healthyWithBuffer = healthyWithBuffer;
    this.healthy = healthy;
  }

  public UsageBucket computeBucket(HotResourceSummary resourceSummary) {
    double value = resourceSummary.getValue();
    if (value <= underProvisioned) {
      return UsageBucket.UNDER_PROVISIONED;
    } else if (value <= healthyWithBuffer) {
      return UsageBucket.HEALTHY_WITH_BUFFER;
    } else if (value <= healthy) {
      return UsageBucket.HEALTHY;
    } else {
      return UsageBucket.UNHEALTHY;
    }
  }
}
