package com.amazon.opendistro.elasticsearch.performanceanalyzer.config.overrides;

/**
 * Class responsible for holding the latest config overrides across the cluster.
 */
public class ConfigOverridesWrapper {

    private volatile ConfigOverrides currentClusterConfigOverrides;
    private volatile long lastUpdatedTimestamp;

    public ConfigOverrides getCurrentClusterConfigOverrides() {
        return currentClusterConfigOverrides;
    }

    /**
     * Sets a new ConfigOverrides instance as the current cluster config overrides instance.
     *
     * @param configOverrides the ConfigOverrides instance.
     */
    public void setCurrentClusterConfigOverrides(final ConfigOverrides configOverrides) {
        this.currentClusterConfigOverrides = configOverrides;
    }

    public long getLastUpdatedTimestamp() {
        return lastUpdatedTimestamp;
    }

    public void setLastUpdatedTimestamp(long lastUpdatedTimestamp) {
        this.lastUpdatedTimestamp = lastUpdatedTimestamp;
    }
}
