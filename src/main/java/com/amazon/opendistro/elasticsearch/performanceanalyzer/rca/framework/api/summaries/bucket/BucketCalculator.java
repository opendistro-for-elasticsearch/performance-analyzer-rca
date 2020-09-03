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

/**
 * A BucketCalculator identifies which {@link UsageBucket} a {@link Resource} should be placed
 * into given that resource's value. It does this on a per-node basis.
 *
 * <p>E.g. a BucketCalculator can compute a value of HEALTHY for CPU on Node "A" and a value of
 * HEALTHY_WITH_BUFFER for CPU on Node "B". A consumer of this information can then read out these
 * bucket values by calling something like getUsageBucket(NodeA, CPU).
 */
interface BucketCalculator {
  /**
   * Identifies which {@link UsageBucket} a {@link Resource} should be placed
   * in given that resource's value.
   *
   * @param resource The resource to check
   * @param value The metric value of the resource (this may be a percentage, duration, etc.) it's
   *              up to the implementation how to handle this value for a particular {@link Resource}
   * @return The {@link UsageBucket} that the {@link Resource} should be associated with
   */
  UsageBucket compute(ResourceEnum resource, double value);
}
