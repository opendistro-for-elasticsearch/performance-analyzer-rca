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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.configs;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.ResourceEnum;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.Config;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.NestedConfig;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.RcaConf;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Defines config for threadpool queue related actions
 *
 * <p>com.amazon.opendistro.elasticsearch.performanceanalyzer.collections.TimeExpiringSetTestConfigs are expected in the following json format:
 * {
 *   "action-config-settings": {
 *     // Queue Capacity bounds are expressed as absolute queue size
 *     "queue-settings": {
 *       "search": {
 *         "upper-bound": 3000,
 *         "lower-bound": 1000
 *       },
 *       "write": {
 *         "upper-bound": 1000,
 *         "lower-bound": 50
 *       }
 *     }
 * }
 */
public class QueueActionConfig {

  private static final Logger LOG = LogManager.getLogger(QueueActionConfig.class);

  private NestedConfig queueSettingsConfig;
  private SearchQueueConfig searchQueueConfig;
  private WriteQueueConfig writeQueueConfig;
  private Map<ResourceEnum, ThresholdConfig<Integer>> thresholdConfigMap;

  public QueueActionConfig(RcaConf conf) {
    Map<String, Object> actionConfig = conf.getActionConfigSettings();
    queueSettingsConfig = new NestedConfig("queue-settings", actionConfig);
    searchQueueConfig = new SearchQueueConfig(queueSettingsConfig);
    writeQueueConfig = new WriteQueueConfig(queueSettingsConfig);
    createThresholdConfigMap();
  }

  public ThresholdConfig<Integer> getThresholdConfig(ResourceEnum threadPool) {
    if (!thresholdConfigMap.containsKey(threadPool)) {
      String msg = "Threshold config requested for unknown threadpool queue: " + threadPool.toString();
      LOG.error(msg);
      throw new IllegalArgumentException(msg);
    }
    return thresholdConfigMap.get(threadPool);
  }

  private void createThresholdConfigMap() {
    Map<ResourceEnum, ThresholdConfig<Integer>> configMap = new HashMap<>();
    configMap.put(ResourceEnum.SEARCH_THREADPOOL, searchQueueConfig);
    configMap.put(ResourceEnum.WRITE_THREADPOOL, writeQueueConfig);
    thresholdConfigMap = Collections.unmodifiableMap(configMap);
  }

  private static class SearchQueueConfig implements ThresholdConfig<Integer> {

    private Config<Integer> searchQueueUpperBound;
    private Config<Integer> searchQueueLowerBound;

    public SearchQueueConfig(NestedConfig queueSettingsConfig) {
      NestedConfig searchQueueConfig = new NestedConfig("search", queueSettingsConfig.getValue());
      searchQueueUpperBound = new Config<>("upper-bound", searchQueueConfig.getValue(),
          3000, (s) -> (s >= 0), Integer.class);
      searchQueueLowerBound = new Config<>("lower-bound", searchQueueConfig.getValue(),
          500, (s) -> (s >= 0), Integer.class);
    }

    @Override
    public Integer upperBound() {
      return searchQueueUpperBound.getValue();
    }

    @Override
    public Integer lowerBound() {
      return searchQueueLowerBound.getValue();
    }
  }

  private static class WriteQueueConfig implements ThresholdConfig<Integer> {

    private Config<Integer> writeQueueUpperBound;
    private Config<Integer> writeQueueLowerBound;

    public WriteQueueConfig(NestedConfig queueSettingsConfig) {
      NestedConfig writeQueueConfig = new NestedConfig("write", queueSettingsConfig.getValue());
      writeQueueUpperBound = new Config<>("upper-bound", writeQueueConfig.getValue(),
          1000, (s) -> (s >= 0), Integer.class);
      writeQueueLowerBound = new Config<>("lower-bound", writeQueueConfig.getValue(),
          50, (s) -> (s >= 0), Integer.class);
    }

    @Override
    public Integer upperBound() {
      return writeQueueUpperBound.getValue();
    }

    @Override
    public Integer lowerBound() {
      return writeQueueLowerBound.getValue();
    }
  }
}
