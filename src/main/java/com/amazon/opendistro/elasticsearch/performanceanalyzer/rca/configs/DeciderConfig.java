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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.configs;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.configs.decider.WorkLoadTypeConfig;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.configs.decider.jvm.OldGenDecisionPolicyConfig;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.RcaConf;

import java.util.Arrays;
import java.util.List;

public class DeciderConfig {

    private static final String CACHE_CONFIG_NAME = "cache-type";
    private static final String THREAD_POOL_CONFIG_NAME = "threadpool-config";
    private static final String WORKLOAD_CONFIG_NAME = "workload-type";
    private static final String OLD_GEN_DECISION_POLICY_CONFIG_NAME = "old-gen-decision-policy-config";
    private static final String PRIORITY_ORDER_CONFIG_NAME = "priority-order";
    // Defaults based on prioritising Stability over performance.
    private static final List<String> DEFAULT_CACHE_PRIORITY = Arrays.asList("fielddata-cache", "shard-request-cache",
            "query-cache", "bitset-filter-cache");

    private List<String> cachePriorityOrder;
    private final WorkLoadTypeConfig workLoadTypeConfig;
    private final OldGenDecisionPolicyConfig oldGenDecisionPolicyConfig;

    public DeciderConfig(final RcaConf rcaConf) {
        cachePriorityOrder = rcaConf.readDeciderConfig(CACHE_CONFIG_NAME,
                PRIORITY_ORDER_CONFIG_NAME, List.class);
        workLoadTypeConfig = new WorkLoadTypeConfig(rcaConf.readDeciderConfig(WORKLOAD_CONFIG_NAME));
        oldGenDecisionPolicyConfig = new OldGenDecisionPolicyConfig(rcaConf.readDeciderConfig(OLD_GEN_DECISION_POLICY_CONFIG_NAME));
        if (cachePriorityOrder == null) {
            cachePriorityOrder = DEFAULT_CACHE_PRIORITY;
        }
    }

    public List<String> getCachePriorityOrder() {
        return cachePriorityOrder;
    }

    public WorkLoadTypeConfig getWorkLoadTypeConfig() {
        return workLoadTypeConfig;
    }

    public OldGenDecisionPolicyConfig getOldGenDecisionPolicyConfig() {
        return oldGenDecisionPolicyConfig;
    }

    public static List<String> getDefaultCachePriority() {
        return DEFAULT_CACHE_PRIORITY;
    }

    public static String getCacheConfigName() {
        return CACHE_CONFIG_NAME;
    }

    public static String getWorkloadConfigName() {
        return WORKLOAD_CONFIG_NAME;
    }

    public static String getPriorityOrderConfigName() {
        return PRIORITY_ORDER_CONFIG_NAME;
    }

}
