package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.rca_publisher;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.GenericSummary;

public interface ClusterSummaryListener <T extends GenericSummary> {
    /**
     * Called ClusterReaderRca emits RCA cluster summary
     */
    void summaryPublished(ClusterSummary<T> clusterSummary);
}
