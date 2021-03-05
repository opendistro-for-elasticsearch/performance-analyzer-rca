package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Metric;

public class ClusterApplierService_Failure extends Metric {
    public ClusterApplierService_Failure(long evaluationIntervalSeconds) {
        super(AllMetrics.ClusterApplierServiceStatsValue.CLUSTER_APPLIER_SERVICE_FAILURE.name(), evaluationIntervalSeconds);
    }
}
