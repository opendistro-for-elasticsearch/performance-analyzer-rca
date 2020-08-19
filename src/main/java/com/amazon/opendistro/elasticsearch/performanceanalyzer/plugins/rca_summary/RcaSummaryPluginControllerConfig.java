package com.amazon.opendistro.elasticsearch.performanceanalyzer.plugins.rca_summary;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.plugins.Plugin;

import java.util.ArrayList;
import java.util.List;

public class RcaSummaryPluginControllerConfig {

    private List<Class<? extends Plugin>> frameworkPlugins;

    public RcaSummaryPluginControllerConfig() {
        frameworkPlugins = new ArrayList<>();
        frameworkPlugins.add(SummaryListenerPlugin.class);
    }

    /**
     * Returns a list of entry point classes for internal framework plugins
     */
    public List<Class<? extends Plugin>> getFrameworkPlugins() {
        return frameworkPlugins;
    }
}

