package com.amazon.opendistro.elasticsearch.performanceanalyzer.plugins.rca_summary;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.plugins.Plugin;

import java.util.ArrayList;
import java.util.List;

public class ClusterRcaPublisherControllerConfig {

    private List<Class<? extends Plugin>> frameworkPlugins;

    public ClusterRcaPublisherControllerConfig() {
        frameworkPlugins = new ArrayList<>();
        frameworkPlugins.add(ClusterSummaryLogger.class);
        frameworkPlugins.add(ClusterSummaryKafkaPublisher.class);
    }

    /**
     * Returns a list of entry point classes for internal framework plugins
     */
    public List<Class<? extends Plugin>> getFrameworkPlugins() {
        return frameworkPlugins;
    }
}

