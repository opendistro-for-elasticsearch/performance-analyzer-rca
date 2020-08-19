package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.configs.decider.jvm;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.configs.ConfigUtils;
import java.util.Map;

public class LevelTwoActionBuilderConfig {
  public static final double DEFAULT_FIELD_DATA_CACHE_LOWER_BOUND = 0.02;
  public static final double DEFAULT_SHARD_REQUEST_CACHE_LOWER_BOUND = 0.01;
  public static final int DEFAULT_CACHE_ACTION_STEP_COUNT = 2;
  public static final int DEFAULT_QUEUE_ACTION_STEP_COUNT = 1;
  public static final int DEFAULT_QUEUE_BUCKET_SIZE = 10;
  private static String FIELD_DATA_CACHE_LOWER_BOUND_CONFIG_NAME = "fielddata-cache-lower-bound";
  private static String SHARD_REQUEST_CACHE_LOWER_BOUND_CONFIG_NAME = "shard-request-cache-lower-bound";
  private static String QUEUE_BUCKET_SIZE_CONFIG_NAME = "queue-bucket-size";
  private Double fieldDataCacheLowerBound;
  private Double shardRequestCacheLowerBound;
  private Integer queueBucketSize;

  public LevelTwoActionBuilderConfig(Map<String, Object> configs) {
    fieldDataCacheLowerBound = ConfigUtils
        .readConfig(configs, FIELD_DATA_CACHE_LOWER_BOUND_CONFIG_NAME, Double.class);
    if (fieldDataCacheLowerBound == null) {
      fieldDataCacheLowerBound = DEFAULT_FIELD_DATA_CACHE_LOWER_BOUND;
    }
    shardRequestCacheLowerBound = ConfigUtils
        .readConfig(configs, SHARD_REQUEST_CACHE_LOWER_BOUND_CONFIG_NAME, Double.class);
    if (shardRequestCacheLowerBound == null) {
      shardRequestCacheLowerBound = DEFAULT_SHARD_REQUEST_CACHE_LOWER_BOUND;
    }
    queueBucketSize = ConfigUtils
        .readConfig(configs, QUEUE_BUCKET_SIZE_CONFIG_NAME, Integer.class);
    if (queueBucketSize == null) {
      queueBucketSize = DEFAULT_QUEUE_BUCKET_SIZE;
    }
  }

  public double fieldDataCacheLowerBound() {
    return fieldDataCacheLowerBound;
  }

  public double shardRequestCacheLowerBound() {
    return shardRequestCacheLowerBound;
  }

  public int queueBucketSize() {
    return queueBucketSize;
  }
}
