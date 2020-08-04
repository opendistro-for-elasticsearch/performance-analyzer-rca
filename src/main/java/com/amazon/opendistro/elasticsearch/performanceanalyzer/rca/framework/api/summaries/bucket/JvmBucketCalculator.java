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
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.ResourceUtil;

/**
 * JvmBucketCalculator is a {@link BucketCalculator} that contains special logic for
 * identifying which buckets the {@link ResourceUtil#YOUNG_GEN_PROMOTION_RATE} and
 * {@link ResourceUtil#OLD_GEN_HEAP_USAGE} resources should be placed into. It returns
 * {@link UsageBucket#UNKNOWN} for all other {@link Resource}s.
 */
public class JvmBucketCalculator extends BucketCalculator {
  private BucketCalculator youngGenPromotionRateThresholds;
  private BucketCalculator oldGenHeapUsageThresholds;

  public JvmBucketCalculator(BucketCalculator youngGenPromotionRateThresholds,
      BucketCalculator oldGenHeapUsageThresholds) {
    this.youngGenPromotionRateThresholds = youngGenPromotionRateThresholds;
    this.oldGenHeapUsageThresholds = oldGenHeapUsageThresholds;
  }

  /**
   * Identifies which buckets a {@link ResourceEnum#YOUNG_GEN} or {@link ResourceEnum#OLD_GEN}
   * resource should be placed into. It returns {@link UsageBucket#UNKNOWN} for all other {@link Resource}s.
   *
   * @param resource Either a {@link ResourceEnum#YOUNG_GEN} or
   *                 {@link ResourceEnum#OLD_GEN}
   * @param value The metric value of the {@link ResourceEnum}
   * @return Which bucket a {@link ResourceEnum#YOUNG_GEN} or {@link ResourceEnum#OLD_GEN} resource
   *         should be placed into or {@link UsageBucket#UNKNOWN} for all other {@link ResourceEnum}s
   */
  @Override
  public UsageBucket compute(ResourceEnum resource, double value) {
    if (resource.equals(ResourceEnum.YOUNG_GEN)) {
      return youngGenPromotionRateThresholds.compute(resource, value);
    } else if (resource.equals(ResourceEnum.OLD_GEN)) {
      return oldGenHeapUsageThresholds.compute(resource, value);
    }
    return UsageBucket.UNKNOWN;
  }
}
