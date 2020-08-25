/*
 * Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 *  permissions and limitations under the License.
 */

package com.amazon.opendistro.elasticsearch.performanceanalyzer.plugins.cluster_rca_publisher;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.plugins.Plugin;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.rca_publisher.ClusterRcaPublisher;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.rca_publisher.ClusterSummaryListener;
import com.google.common.annotations.VisibleForTesting;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ClusterRcaPublisherController {
    private static final Logger LOG = LogManager.getLogger(ClusterRcaPublisherController.class);
    private ClusterRcaPublisher rca;
    private List<Plugin> plugins;
    private ClusterRcaPublisherControllerConfig clusterRcaPublisherControllerConfig;

    public ClusterRcaPublisherController(ClusterRcaPublisherControllerConfig pluginConfig, ClusterRcaPublisher rca) {
        this.clusterRcaPublisherControllerConfig = pluginConfig;
        this.rca = rca;
        this.plugins = new ArrayList<>();
    }

    public void initPlugins() {
        loadFrameworkPlugins();
        registerClusterSummaryListener();
    }

    private void loadFrameworkPlugins() {
        for (Class<?> pluginClass : clusterRcaPublisherControllerConfig.getFrameworkPlugins()) {
            final Constructor<?>[] constructors = pluginClass.getConstructors();
            if (constructors.length == 0) {
                throw new IllegalStateException(
                        "no public constructor found for plugin class: [" + pluginClass.getName() + "]");
            }
            if (constructors.length > 1) {
                throw new IllegalStateException(
                        "unique constructor expected for plugin class: [" + pluginClass.getName() + "]");
            }
            if (constructors[0].getParameterCount() != 0) {
                throw new IllegalStateException(
                        "default constructor expected for plugin class: [" + pluginClass.getName() + "]");
            }

            try {
                plugins.add((Plugin) constructors[0].newInstance());
                LOG.info("loaded plugin: [{}]", plugins.get(plugins.size() - 1).name());
            } catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
                LOG.error("Failed to instantiate plugin", e);
                throw new IllegalStateException("Failed to instantiate plugin: [" + pluginClass.getName() + "]", e);
            }
        }
    }

    private void registerClusterSummaryListener() {
        for (Plugin plugin : plugins) {
            if (ClusterSummaryListener.class.isAssignableFrom(plugin.getClass())) {
                rca.addClusterSummaryListener((ClusterSummaryListener) plugin);
            }
        }
    }

    @VisibleForTesting
    public List<Plugin> getPlugins() {
        return plugins;
    }
}