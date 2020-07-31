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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.config.overrides;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Class responsible for holding the latest config overrides across the cluster.
 */
public class ConfigOverridesWrapper {

    private volatile ConfigOverrides currentClusterConfigOverrides;
    private volatile long lastUpdatedTimestamp;
    private final ObjectMapper mapper;

    public ConfigOverridesWrapper() {
        this(new ObjectMapper());
    }

    /**
     * Ctor used only for unit test purposes.
     * @param mapper The object mapper instance.
     */
    public ConfigOverridesWrapper(final ObjectMapper mapper) {
        this.currentClusterConfigOverrides = new ConfigOverrides();
        this.mapper = mapper;
    }

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
