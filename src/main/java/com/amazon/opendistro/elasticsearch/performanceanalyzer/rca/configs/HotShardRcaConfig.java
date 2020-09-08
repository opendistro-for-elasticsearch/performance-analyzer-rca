package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.configs;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.RcaConf;

public class HotShardRcaConfig {
    public static final String CONFIG_NAME = "hot-shard-rca";

    private Double cpuUtilizationThreshold;
    private Double ioTotThroughputThreshold;
    private Double ioTotSysCallRateThreshold;

    public static final double DEFAULT_CPU_UTILIZATION_THRESHOLD = 0.01;
    public static final double DEFAULT_IO_TOTAL_THROUGHPUT_THRESHOLD_IN_BYTE_PER_SEC = 250000.0;
    public static final double DEFAULT_IO_TOTAL_SYSCALL_RATE_THRESHOLD_PER_SEC = 0.01;

    public HotShardRcaConfig(final RcaConf rcaConf) {
        cpuUtilizationThreshold = rcaConf.readRcaConfig(CONFIG_NAME,
                HotShardRcaConfig.RCA_CONF_KEY_CONSTANTS.CPU_UTILIZATION_THRESHOLD,
                DEFAULT_CPU_UTILIZATION_THRESHOLD, (s) -> (s > 0), Double.class);
        ioTotThroughputThreshold = rcaConf.readRcaConfig(CONFIG_NAME,
                HotShardRcaConfig.RCA_CONF_KEY_CONSTANTS.IO_TOT_THROUGHPUT_THRESHOLD_IN_BYTES,
                DEFAULT_IO_TOTAL_THROUGHPUT_THRESHOLD_IN_BYTE_PER_SEC, (s) -> (s > 0), Double.class);
        ioTotSysCallRateThreshold = rcaConf.readRcaConfig(CONFIG_NAME,
                HotShardRcaConfig.RCA_CONF_KEY_CONSTANTS.IO_TOT_SYSCALL_RATE_THRESHOLD_PER_SECOND,
                DEFAULT_IO_TOTAL_SYSCALL_RATE_THRESHOLD_PER_SEC, (s) -> (s > 0), Double.class);
    }

    public double getCpuUtilizationThreshold() {
        return cpuUtilizationThreshold;
    }

    public double getIoTotThroughputThreshold() {
        return ioTotThroughputThreshold;
    }

    public double getIoTotSysCallRateThreshold() {
        return ioTotSysCallRateThreshold;
    }

    public static class RCA_CONF_KEY_CONSTANTS {
        public static final String CPU_UTILIZATION_THRESHOLD = "cpu-utilization";
        public static final String IO_TOT_THROUGHPUT_THRESHOLD_IN_BYTES = "io-total-throughput-in-bytes";
        public static final String IO_TOT_SYSCALL_RATE_THRESHOLD_PER_SECOND = "io-total-syscallrate-per-second";
    }
}
