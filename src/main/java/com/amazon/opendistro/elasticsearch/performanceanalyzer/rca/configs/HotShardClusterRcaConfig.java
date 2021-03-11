/*
 * Copyright 2021 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.RcaConf;

public class HotShardClusterRcaConfig {
    public static final String CONFIG_NAME = "hot-shard-cluster-rca";

    private Double cpuUtilizationClusterThreshold;
    private Double ioTotThroughputClusterThreshold;
    private Double ioTotSysCallRateClusterThreshold;

    public static final double DEFAULT_CPU_UTILIZATION_CLUSTER_THRESHOLD = 0.3;
    public static final double DEFAULT_IO_TOTAL_THROUGHPUT_CLUSTER_THRESHOLD = 0.3;
    public static final double DEFAULT_IO_TOTAL_SYSCALL_RATE_CLUSTER_THRESHOLD = 0.3;

    public HotShardClusterRcaConfig(final RcaConf rcaConf) {
        cpuUtilizationClusterThreshold = rcaConf.readRcaConfig(CONFIG_NAME,
                HotShardClusterRcaConfig.RCA_CONF_KEY_CONSTANTS.CPU_UTILIZATION_CLUSTER_THRESHOLD,
                DEFAULT_CPU_UTILIZATION_CLUSTER_THRESHOLD, (s) -> (s > 0), Double.class);
        ioTotThroughputClusterThreshold = rcaConf.readRcaConfig(CONFIG_NAME,
                HotShardClusterRcaConfig.RCA_CONF_KEY_CONSTANTS.CLUSTER_IO_THROUGHPUT_CLUSTER_THRESHOLD,
                DEFAULT_IO_TOTAL_THROUGHPUT_CLUSTER_THRESHOLD, (s) -> (s > 0), Double.class);
        ioTotSysCallRateClusterThreshold = rcaConf.readRcaConfig(CONFIG_NAME,
                HotShardClusterRcaConfig.RCA_CONF_KEY_CONSTANTS.CLUSTER_IO_SYSCALLRATE_CLUSTER_THRESHOLD,
                DEFAULT_IO_TOTAL_SYSCALL_RATE_CLUSTER_THRESHOLD, (s) -> (s > 0), Double.class);
    }

    public double getCpuUtilizationClusterThreshold() {
        return cpuUtilizationClusterThreshold;
    }

    public double getIoTotThroughputClusterThreshold() {
        return ioTotThroughputClusterThreshold;
    }

    public double getIoTotSysCallRateClusterThreshold() {
        return ioTotSysCallRateClusterThreshold;
    }

    public static class RCA_CONF_KEY_CONSTANTS {
        private static final String CPU_UTILIZATION_CLUSTER_THRESHOLD = "cpu-utilization-cluster-percentage";
        private static final String CLUSTER_IO_THROUGHPUT_CLUSTER_THRESHOLD =  "io-total-throughput-cluster-percentage";
        private static final String CLUSTER_IO_SYSCALLRATE_CLUSTER_THRESHOLD = "io-total-syscallrate-cluster-percentage";
    }
}
