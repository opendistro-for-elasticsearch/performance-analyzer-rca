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
 * permissions and limitations under the License.
 */

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.bucket;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.Resource;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.ResourceEnum;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.ResourceUtil;
import java.util.Objects;

/**
 * JvmBucketCalculator is a {@link BucketCalculator} that contains special logic for
 * identifying which buckets the {@link ResourceUtil#YOUNG_GEN_PROMOTION_RATE} and
 * {@link ResourceUtil#OLD_GEN_HEAP_USAGE} resources should be placed into. It returns
 * {@link UsageBucket#UNKNOWN} for all other {@link Resource}s.
 */
public class JvmBucketCalculator implements BucketCalculator {
  private BucketCalculator youngGenCalculator;
  private BucketCalculator oldGenCalculator;

  public JvmBucketCalculator(BucketCalculator youngGenCalculator,
      BucketCalculator oldGenCalculator) {
    this.youngGenCalculator = youngGenCalculator;
    this.oldGenCalculator = oldGenCalculator;
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
      return youngGenCalculator.compute(resource, value);
    } else if (resource.equals(ResourceEnum.OLD_GEN)) {
      return oldGenCalculator.compute(resource, value);
    }
    return UsageBucket.UNKNOWN;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    JvmBucketCalculator that = (JvmBucketCalculator) o;
    return Objects
        .equals(youngGenCalculator, that.youngGenCalculator)
        && Objects.equals(oldGenCalculator, that.oldGenCalculator);
  }

  @Override
  public int hashCode() {
    return Objects.hash(youngGenCalculator, oldGenCalculator);
  }
}
