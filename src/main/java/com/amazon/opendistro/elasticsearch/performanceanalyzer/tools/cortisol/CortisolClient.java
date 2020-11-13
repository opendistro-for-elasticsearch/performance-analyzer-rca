package com.amazon.opendistro.elasticsearch.performanceanalyzer.tools.cortisol;

/**
 * The client interface for Cortisol.
 */
public interface CortisolClient {
    /**
     * Runs an ingest workload as specified by the bulkLoadParams object.
     * @param bulkLoadParams The object specifying the parameters for the stress test.
     */
    void stressBulk(final BulkLoadParams bulkLoadParams);

    /**
     * Runs a search workload as specified by the searchLoadParams object.
     * @param searchLoadParams The object specifying the parameters for the stress test.
     */
    void stressSearch(final SearchLoadParams searchLoadParams);

    /**
     * Cleans up the cluster by deleting the indices created for stress testing.
     */
    void cleanup();
}
