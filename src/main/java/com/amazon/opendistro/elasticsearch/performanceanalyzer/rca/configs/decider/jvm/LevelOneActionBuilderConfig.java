package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.configs.decider.jvm;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.configs.ConfigUtils;
import java.util.Map;

public class LevelOneActionBuilderConfig {
  public static final double DEFAULT_FIELD_DATA_CACHE_LOWER_BOUND = 0.1;
  public static final double DEFAULT_SHARD_REQUEST_CACHE_LOWER_BOUND = 0.02;
  private static String FIELD_DATA_CACHE_LOWER_BOUND_CONFIG_NAME = "fielddata-cache-lower-bound";
  private static String SHARD_REQUEST_CACHE_LOWER_BOUND_CONFIG_NAME = "shard-request-cache-lower-bound";
  private Double fieldDataCacheLowerBound;
  private Double shardRequestCacheLowerBound;

  public LevelOneActionBuilderConfig(Map<String, Object> configs) {
    fieldDataCacheLowerBound = ConfigUtils.readConfig(configs, FIELD_DATA_CACHE_LOWER_BOUND_CONFIG_NAME, Double.class);
    if (fieldDataCacheLowerBound == null) {
      fieldDataCacheLowerBound = DEFAULT_FIELD_DATA_CACHE_LOWER_BOUND;
    }
    shardRequestCacheLowerBound = ConfigUtils.readConfig(configs, SHARD_REQUEST_CACHE_LOWER_BOUND_CONFIG_NAME, Double.class);
    if (shardRequestCacheLowerBound == null) {
      shardRequestCacheLowerBound = DEFAULT_SHARD_REQUEST_CACHE_LOWER_BOUND;
    }
  }

  public double fieldDataCacheLowerBound() {
    return fieldDataCacheLowerBound;
  }

  public double shardRequestCacheLowerBound() {
    return shardRequestCacheLowerBound;
  }
}
