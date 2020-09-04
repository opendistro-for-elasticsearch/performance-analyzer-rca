/*
 * Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.deciders.configs;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.deciders.configs.jvm.OldGenDecisionPolicyConfig;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.NestedConfig;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.RcaConf;

/**
 * "decider-config-settings": {
 *     // Decreasing order of priority for the type of workload we can expect on the cluster.
 *     // Priority order in the list goes from most expected to the lease expected workload type.
 *     "workload-type": {
 *       "prefer-ingest": true,
 *       "prefer-search": false
 *     },
 *     // Decreasing order of priority for the type of cache which is expected to be consumed more.
 *     // Priority order in the list goes from most used to the lease used cache type.
 *     "cache-type": {
 *       "priority-order": ["fielddata-cache", "shard-request-cache", "query-cache", "bitset-filter-cache"]
 *     },
 *     "old-gen-decision-policy-config": {
 *       XXXX
 *     }
 *   },
 */
public class DeciderConfig {

    private static final String CACHE_CONFIG_NAME = "cache-type";
    private static final String WORKLOAD_CONFIG_NAME = "workload-type";
    private static final String OLD_GEN_DECISION_POLICY_CONFIG_NAME = "old-gen-decision-policy-config";

    private final CachePriorityOrderConfig cachePriorityOrderConfig;
    private final WorkLoadTypeConfig workLoadTypeConfig;
    private final OldGenDecisionPolicyConfig oldGenDecisionPolicyConfig;

    public DeciderConfig(final RcaConf rcaConf) {
        cachePriorityOrderConfig = new CachePriorityOrderConfig(
            new NestedConfig(CACHE_CONFIG_NAME, rcaConf.getDeciderConfigSettings())
        );
        workLoadTypeConfig = new WorkLoadTypeConfig(
            new NestedConfig(WORKLOAD_CONFIG_NAME, rcaConf.getDeciderConfigSettings())
        );
        oldGenDecisionPolicyConfig = new OldGenDecisionPolicyConfig(
            new NestedConfig(OLD_GEN_DECISION_POLICY_CONFIG_NAME, rcaConf.getDeciderConfigSettings())
        );
    }

    public CachePriorityOrderConfig getCachePriorityOrderConfig() {
        return cachePriorityOrderConfig;
    }

    public WorkLoadTypeConfig getWorkLoadTypeConfig() {
        return workLoadTypeConfig;
    }

    public OldGenDecisionPolicyConfig getOldGenDecisionPolicyConfig() {
        return oldGenDecisionPolicyConfig;
    }
}
