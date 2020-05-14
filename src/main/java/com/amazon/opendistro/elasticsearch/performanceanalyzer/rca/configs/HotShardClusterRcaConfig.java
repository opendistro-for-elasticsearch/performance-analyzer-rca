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
                HotShardClusterRcaConfig.RCA_CONF_KEY_CONSTANTS.CPU_UTILIZATION_CLUSTER_THRESHOLD, Double.class);
        ioTotThroughputClusterThreshold = rcaConf.readRcaConfig(CONFIG_NAME,
                HotShardClusterRcaConfig.RCA_CONF_KEY_CONSTANTS.CLUSTER_IO_THROUGHPUT_CLUSTER_THRESHOLD, Double.class);
        ioTotSysCallRateClusterThreshold = rcaConf.readRcaConfig(CONFIG_NAME,
                HotShardClusterRcaConfig.RCA_CONF_KEY_CONSTANTS.CLUSTER_IO_SYSCALLRATE_CLUSTER_THRESHOLD, Double.class);
        if (cpuUtilizationClusterThreshold == null) {
            cpuUtilizationClusterThreshold = DEFAULT_CPU_UTILIZATION_CLUSTER_THRESHOLD;
        }
        if (ioTotThroughputClusterThreshold == null) {
            ioTotThroughputClusterThreshold = DEFAULT_IO_TOTAL_THROUGHPUT_CLUSTER_THRESHOLD;
        }
        if (ioTotSysCallRateClusterThreshold == null) {
            ioTotSysCallRateClusterThreshold = DEFAULT_IO_TOTAL_SYSCALL_RATE_CLUSTER_THRESHOLD;
        }
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
