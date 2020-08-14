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

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.contexts.ResourceContext;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotClusterSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.bucket.BucketCalculator;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.bucket.StaticBucketCalculator;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.RcaConf;
import java.util.List;

public class CpuClusterResourceFlowUnit extends ClusterResourceFlowUnit {
  public CpuClusterResourceFlowUnit(long timeStamp) {
    super(timeStamp);
  }

  public CpuClusterResourceFlowUnit(long timeStamp,
      ResourceContext context,
      HotClusterSummary summary,
      RcaConf rcaConf,
      boolean persistSummary) {
    super(timeStamp, context, summary, rcaConf, persistSummary);
  }

  @Override
  public BucketCalculator initBucketCalculator() {
    if (rcaConf == null) {
      return null;
    }
    List<Double> cpuUsageThresholds = rcaConf.getUsageBucketThresholds(BucketCalculator.CPU_USAGE);
    return new StaticBucketCalculator(cpuUsageThresholds.get(0), cpuUsageThresholds.get(1),
        cpuUsageThresholds.get(2));
  }
}
