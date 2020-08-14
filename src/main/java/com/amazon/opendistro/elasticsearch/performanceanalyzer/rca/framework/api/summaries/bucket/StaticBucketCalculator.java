/*
 * Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 *  permissions and limitations under the License.
 */

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.bucket;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.Resource;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.ResourceEnum;
import java.util.Objects;

/**
 * StaticBucketCalculator is a {@link BucketCalculator} which places {@link Resource}s into
 * {@link UsageBucket}s based on statically defined ranges.
 */
public class StaticBucketCalculator extends BucketCalculator {
  // a value in (-inf, healthyWithBuffer] is considered healthy with
  // a buffer for increasing utilization
  double healthyWithBuffer;
  // a value in (healthyWithBuffer, healthy] is considered healthy, and this implies that a value
  // in (healthy, inf) is considered unhealthy
  double healthy;
  // a value in (healthy, unhealthy] is considered under provisioned
  // a value in (unhealthy, inf] is considered unhealthy
  double unhealthy;

  public StaticBucketCalculator(double healthyWithBuffer, double healthy, double unhealthy) {
    this.healthyWithBuffer = healthyWithBuffer;
    this.healthy = healthy;
    this.unhealthy = unhealthy;
  }

  @Override
  public UsageBucket compute(ResourceEnum resource, double value) {
    if (value <= healthyWithBuffer) {
      return UsageBucket.HEALTHY_WITH_BUFFER;
    } else if (value <= healthy) {
      return UsageBucket.HEALTHY;
    } else if (value <= unhealthy) {
      return UsageBucket.UNDER_PROVISIONED;
    } else {
        return UsageBucket.UNHEALTHY;
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    StaticBucketCalculator that = (StaticBucketCalculator) o;
    return Double.compare(that.healthyWithBuffer, healthyWithBuffer) == 0
        && Double.compare(that.healthy, healthy) == 0
        && Double.compare(that.unhealthy, unhealthy) == 0;
  }

  @Override
  public int hashCode() {
    return Objects.hash(healthyWithBuffer, healthy, unhealthy);
  }
}
