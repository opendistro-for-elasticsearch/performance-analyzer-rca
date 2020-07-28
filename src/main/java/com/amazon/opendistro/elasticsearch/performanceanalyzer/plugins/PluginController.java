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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.plugins;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.ActionListener;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.deciders.Publisher;
import com.google.common.annotations.VisibleForTesting;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class PluginController {

  private static final Logger LOG = LogManager.getLogger(PluginController.class);
  private final Publisher publisher;
  private List<Plugin> plugins;
  private PluginControllerConfig pluginControllerConfig;

  public PluginController(PluginControllerConfig pluginConfig, Publisher publisher) {
    this.pluginControllerConfig = pluginConfig;
    this.publisher = publisher;
    this.plugins = new ArrayList<>();
    loadFrameworkPlugins();
    registerActionListeners();
  }

  private void loadFrameworkPlugins() {
    for (Class<?> pluginClass : pluginControllerConfig.getFrameworkPlugins()) {
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

  private void registerActionListeners() {
    for (Plugin plugin: plugins) {
      if (ActionListener.class.isAssignableFrom(plugin.getClass())) {
        publisher.addActionListener((ActionListener)plugin);
      }
    }
  }

  @VisibleForTesting
  List<Plugin> getPlugins() {
    return plugins;
  }
}
