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

public class CacheActionConfig {

  private NestedConfig cacheSettingsConfig;
  private FieldDataCacheConfig fieldDataCacheConfig;
  private ShardRequestCacheConfig shardRequestCacheConfig;
  private Map<ResourceEnum, CacheMaxSizeActionConfig> cacheMaxSizeActionConfigMap;

  public CacheActionConfig(RcaConf conf) {
    Map<String, Object> actionConfig = conf.getActionConfigSettings();
    cacheSettingsConfig = new NestedConfig("cache-settings", actionConfig);
    fieldDataCacheConfig = new FieldDataCacheConfig(cacheSettingsConfig);
    shardRequestCacheConfig = new ShardRequestCacheConfig(cacheSettingsConfig);
    cacheMaxSizeActionConfigMap = createCacheConfigMap();
  }

  public CacheMaxSizeActionConfig getCacheMaxSizeActionConfig(ResourceEnum cacheType) {
    return cacheMaxSizeActionConfigMap.get(cacheType);
  }

  private Map<ResourceEnum, CacheMaxSizeActionConfig> createCacheConfigMap() {
    Map<ResourceEnum, CacheMaxSizeActionConfig> configMap = new HashMap<>();
    configMap.put(ResourceEnum.FIELD_DATA_CACHE, fieldDataCacheConfig);
    configMap.put(ResourceEnum.SHARD_REQUEST_CACHE, shardRequestCacheConfig);
    return Collections.unmodifiableMap(configMap);
  }


  private interface CacheMaxSizeActionConfig {
    Double upperBound();
    Double lowerBound();
  }

  private static class FieldDataCacheConfig implements CacheMaxSizeActionConfig {

    private Config<Double> fieldDataCacheUpperBound;
    private Config<Double> fieldDataCacheLowerBound;

    public FieldDataCacheConfig(NestedConfig cacheSettingsConfig) {
      NestedConfig fieldDataCacheConfig = new NestedConfig("fielddata", cacheSettingsConfig.getValue());
      fieldDataCacheUpperBound = new Config<>("upper-bound", fieldDataCacheConfig.getValue(), 0.4, Double.class);
      fieldDataCacheLowerBound = new Config<>("lower-bound", fieldDataCacheConfig.getValue(), 0.1, Double.class);
    }

    @Override
    public Double upperBound() {
      return fieldDataCacheUpperBound.getValue();
    }

    @Override
    public Double lowerBound() {
      return fieldDataCacheLowerBound.getValue();
    }
  }

  private static class ShardRequestCacheConfig implements CacheMaxSizeActionConfig {

    private Config<Double> shardRequestCacheUpperBound;
    private Config<Double> shardRequestCacheLowerBound;

    public ShardRequestCacheConfig(NestedConfig cacheSettingsConfig) {
      NestedConfig shardRequestCacheConfig = new NestedConfig("shard-request", cacheSettingsConfig.getValue());
      shardRequestCacheUpperBound = new Config<>("upper-bound", shardRequestCacheConfig.getValue(), 0.05, Double.class);
      shardRequestCacheLowerBound = new Config<>("lower-bound", shardRequestCacheConfig.getValue(), 0.01, Double.class);
    }

    @Override
    public Double upperBound() {
      return shardRequestCacheUpperBound.getValue();
    }

    @Override
    public Double lowerBound() {
      return shardRequestCacheLowerBound.getValue();
    }
  }
}
