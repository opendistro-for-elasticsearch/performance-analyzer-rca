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
 * BasicBucketCalculator is a {@link BucketCalculator} which places {@link Resource}s into
 * {@link UsageBucket}s based on defined ranges.
 */
public class BasicBucketCalculator implements BucketCalculator {
  // a value in (-inf, underUtilized] is considered underutilized and signals that additional
  // resources may be removed for the sake of frugality
  double underUtilized;
  // a value in (underutilized, healthyWithBuffer] is considered healthy, which means that we may
  // be able to increase the pressure on this resource
  double healthyWithBuffer;
  // a value in (healthyWithBuffer, healthy] is considered healthy and we probably shouldn't mess
  // with the resource
  // a value in (healthy, inf] is considered unhealthy and we should find ways to decrease the pressure
  double healthy;

  public BasicBucketCalculator(double underUtilized, double healthyWithBuffer, double healthy) {
    this.underUtilized = underUtilized;
    this.healthyWithBuffer = healthyWithBuffer;
    this.healthy = healthy;
  }

  @Override
  public UsageBucket compute(ResourceEnum resource, double value) {
    if (value <= underUtilized) {
      return UsageBucket.UNDER_UTILIZED;
    } else if (value <= healthyWithBuffer) {
      return UsageBucket.HEALTHY_WITH_BUFFER;
    } else if (value <= healthy) {
      return UsageBucket.HEALTHY;
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
    BasicBucketCalculator that = (BasicBucketCalculator) o;
    return Double.compare(that.underUtilized, underUtilized) == 0
        && Double.compare(that.healthyWithBuffer, healthyWithBuffer) == 0
        && Double.compare(that.healthy, healthy) == 0;
  }

  @Override
  public int hashCode() {
    return Objects.hash(underUtilized, healthyWithBuffer, healthy);
  }
}
