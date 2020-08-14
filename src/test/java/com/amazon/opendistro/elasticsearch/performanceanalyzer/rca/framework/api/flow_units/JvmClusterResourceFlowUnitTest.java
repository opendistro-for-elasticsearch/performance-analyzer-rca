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
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.bucket.JvmBucketCalculator;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.bucket.StaticBucketCalculator;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.RcaConf;
import java.util.List;

public class JvmClusterResourceFlowUnitTest extends ClusterResourceFlowUnitTest {

  @Override
  protected ClusterResourceFlowUnit newEmptyFlowUnit() {
    return new JvmClusterResourceFlowUnit(System.currentTimeMillis());
  }

  @Override
  protected ClusterResourceFlowUnit newFlowUnit(ResourceContext context, HotClusterSummary summary,
      RcaConf rcaConf, boolean persistSummary) {
    return new JvmClusterResourceFlowUnit(System.currentTimeMillis(), context, summary, rcaConf, persistSummary);
  }

  @Override
  protected BucketCalculator expectedBucketCalculator() {
    List<Double> youngGenHeapPromotionRateThresholds = rcaConf.getUsageBucketThresholds(BucketCalculator.YOUNG_GEN_PROMOTION_RATE);
    List<Double> oldGenHeapUsageThresholds = rcaConf.getUsageBucketThresholds(BucketCalculator.OLD_GEN_HEAP_USAGE);
    BucketCalculator youngGenCalculator = new StaticBucketCalculator(
        youngGenHeapPromotionRateThresholds.get(0),
        youngGenHeapPromotionRateThresholds.get(1),
        youngGenHeapPromotionRateThresholds.get(2));
    BucketCalculator oldGenCalculator =
        new StaticBucketCalculator(oldGenHeapUsageThresholds.get(0), oldGenHeapUsageThresholds.get(1),
            oldGenHeapUsageThresholds.get(2));
    return new JvmBucketCalculator(youngGenCalculator, oldGenCalculator);
  }
}
