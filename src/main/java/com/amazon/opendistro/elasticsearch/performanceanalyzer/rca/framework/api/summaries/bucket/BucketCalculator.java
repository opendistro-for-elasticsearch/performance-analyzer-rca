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
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.cluster.NodeKey;
import java.util.HashMap;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * A BucketCalculator identifies which {@link UsageBucket} a {@link Resource} should be placed
 * into given that resource's value. It does this on a per-node basis.
 *
 * <p>E.g. a BucketCalculator can compute a value of HEALTHY for CPU on Node "A" and a value of
 * HEALTHY_WITH_BUFFER for CPU on Node "B". A consumer of this information can then read out these
 * bucket values by calling something like getUsageBucket(NodeA, CPU).
 */
public abstract class BucketCalculator {
  private final Logger LOG = LogManager.getLogger(getClass());
  // Strings which map to threshold definitions in rca.conf
  public static final String YOUNG_GEN_PROMOTION_RATE = "young-gen-heap-promotion-rate";
  public static final String OLD_GEN_HEAP_USAGE = "old-gen-heap-usage";
  public static final String CPU_USAGE = "cpu-usage";

  // For each Node, store a map of resource name to the UsageBucket for that resource
  protected Map<NodeKey, Map<ResourceEnum, UsageBucket>> resourceBucketMap = new HashMap<>();

  public void computeBucket(NodeKey nodeKey, ResourceEnum resource, double value) {
    this.resourceBucketMap.compute(nodeKey, (k, v) -> {
      if (v == null) {
        v = new HashMap<>();
      }
      v.put(resource, compute(resource, value));
      return v;
    });
  }

  /**
   * Identifies which {@link UsageBucket} a {@link Resource} should be placed
   * in given that resource's value.
   *
   * @param resource The resource to check
   * @param value The metric value of the resource (this may be a percentage, duration, etc.) it's
   *              up to the implementation how to handle this value for a particular {@link Resource}
   * @return The {@link UsageBucket} that the {@link Resource} should be associated with
   */
  abstract UsageBucket compute(ResourceEnum resource, double value);

  /**
   * Returns the {@link UsageBucket} for a particular {@link Resource} on a given Node, or null if
   * the bucket for that ({@link NodeKey}, {@link Resource}) tuple was not previously computed.
   *
   * <p>NOTE: this function should return null for any ({@link NodeKey}, {@link Resource}) tuple
   * (k, r) if there was no previous call to computeBucket(k, r, v) where v is any double. In other
   * words, call computeBucket() before calling this function.
   *
   * @param nodeKey A key for the node we're interested in
   * @param resource The resource on the node that we're interested in
   * @return the {@link UsageBucket} for a particular {@link Resource} on a given Node, or null if
   *         the bucket for that ({@link NodeKey}, {@link Resource}) tuple was not previously computed.
   */
  public UsageBucket getUsageBucket(NodeKey nodeKey, ResourceEnum resource) {
    Map<ResourceEnum, UsageBucket> nodeResourceBuckets = resourceBucketMap.get(nodeKey);
    if (nodeResourceBuckets != null) {
      return nodeResourceBuckets.getOrDefault(resource, UsageBucket.UNKNOWN);
    }
    LOG.debug("getUsageBucket called with invalid NodeKey {}", nodeKey);
    return UsageBucket.UNKNOWN;
  }
}
