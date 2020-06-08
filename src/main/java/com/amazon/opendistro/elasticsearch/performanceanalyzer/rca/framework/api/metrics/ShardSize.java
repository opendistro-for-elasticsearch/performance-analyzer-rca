/*
 * Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Metric;

/**
 * This metric is calculated from the Node Stat Metrics for a particular node and returns the
 * per Shard ID and Index Name dimensional shard sizes. This metric is aggregated over all shards
 * in different RCAs(Temperature Profile RCA).
 */

public class ShardSize extends Metric {
    public static final String NAME = AllMetrics.ShardStatsValue.SHARD_SIZE_IN_BYTES.toString();

    public ShardSize(long evaluationIntervalSeconds) {
        super(AllMetrics.ShardStatsValue.SHARD_SIZE_IN_BYTES.name(), evaluationIntervalSeconds);
    }
}
