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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.flow_units;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.ResourceEnum;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.contexts.ResourceContext;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotClusterSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotNodeSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotResourceSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.bucket.BucketCalculator;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.bucket.UsageBucket;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.RcaConf;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.cluster.NodeKey;

/**
 * Base class for cluster level {@link ResourceFlowUnit}s which want to bucketize resources into
 * various {@link UsageBucket}s
 *
 * <p>If you'd like to subclass this class, simply implement initBucketCalculator() to return an
 * appropriate {@link BucketCalculator} to use for bucketization. {@link CpuClusterResourceFlowUnit}
 * is a good example of this.
 */
public class ClusterResourceFlowUnit extends ResourceFlowUnit<HotClusterSummary> {
  // This config is useful for reading bucket ranges from the rca.conf file
  protected RcaConf rcaConf;
  // This calculator is used to bucketize our resources per node
  protected BucketCalculator bucketCalculator;

  public ClusterResourceFlowUnit(long timeStamp) {
    super(timeStamp);
  }

  public ClusterResourceFlowUnit(long timeStamp, ResourceContext context,
      HotClusterSummary summary, RcaConf rcaConf, boolean persistSummary) {
    super(timeStamp, context, summary, persistSummary);
    this.rcaConf = rcaConf;
    this.bucketCalculator = initBucketCalculator();
    computeUsageBuckets();
  }

  /**
   * Returns the {@link BucketCalculator} to use for bucketization of resources
   *
   * @return the {@link BucketCalculator} to use for bucketization of resources
   */
  protected BucketCalculator initBucketCalculator() {
    return null;
  }

  /**
   * Computes which usage bucket each resource in this class's summary should belong to.
   *
   * <p>You probably don't need to override this. Just implement initBucketCalculator()
   */
  protected void computeUsageBuckets() {
    if (getSummary() == null) {
      return;
    }
    for (HotNodeSummary hotNodeSummary : getSummary().getHotNodeSummaryList()) {
      for (HotResourceSummary hotResourceSummary : hotNodeSummary.getHotResourceSummaryList()) {
        NodeKey nodeKey = new NodeKey(hotNodeSummary.getNodeID(), hotNodeSummary.getHostAddress());
        if (bucketCalculator != null) {
          bucketCalculator
              .computeBucket(nodeKey, hotResourceSummary.getResource().getResourceEnum(),
                  hotResourceSummary.getValue());
        }
      }
    }
  }

  /**
   * Returns the {@link UsageBucket} that a resource was placed into for a given node or
   * {@link UsageBucket#UNKNOWN} if this value was never computed
   *
   * @param nodeKey The node whose resource we're interested in
   * @param resource The actual resource we're interested in
   * @return the {@link UsageBucket} that a resource was placed into for a given node or
   *         {@link UsageBucket#UNKNOWN} if this value was never computed
   */
  public UsageBucket getUsageBucket(NodeKey nodeKey, ResourceEnum resource) {
    if (bucketCalculator == null) {
      return UsageBucket.UNKNOWN;
    }
    return bucketCalculator.getUsageBucket(nodeKey, resource);
  }
}
