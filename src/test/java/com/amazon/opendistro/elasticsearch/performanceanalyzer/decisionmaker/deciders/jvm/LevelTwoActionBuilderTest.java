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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.deciders.jvm;

import static com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.cache.CacheUtil.GB_TO_BYTES;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.AppContext;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.Action;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.ModifyCacheMaxSizeAction;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.ModifyQueueCapacityAction;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.configs.CacheActionConfig;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.configs.QueueActionConfig;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.deciders.configs.jvm.LevelTwoActionBuilderConfig;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.deciders.jvm.old_gen.LevelTwoActionBuilder;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.deciders.test_utils.DeciderActionParserUtil;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.ResourceEnum;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.ResourceUtil;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.RcaConf;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.util.InstanceDetails;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.collector.NodeConfigCache;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.cluster.NodeKey;
import java.util.List;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class LevelTwoActionBuilderTest {
  private AppContext testAppContext;
  private NodeConfigCache dummyCache;
  private RcaConf rcaConf;
  private NodeKey node;
  private final long heapMaxSizeInBytes = 10 * GB_TO_BYTES;
  private int writeQueueStep;
  private int searchQueueStep;
  private double fielddataCahceStepSize;
  private double shardRequestCacheStepSize;
  private DeciderActionParserUtil deciderActionParser;

  public LevelTwoActionBuilderTest() {
    testAppContext = new AppContext();
    dummyCache = testAppContext.getNodeConfigCache();
    rcaConf = new RcaConf();
    node = new NodeKey(new InstanceDetails.Id("node-1"), new InstanceDetails.Ip("127.0.0.1"));
    deciderActionParser = new DeciderActionParserUtil();
  }

  @Before
  public void init() throws Exception {
    final String configStr =
        "{"
            + "\"decider-config-settings\": { "
              + "\"old-gen-decision-policy-config\": { "
                + "\"queue-bucket-size\": 5 "
              + "} "
            + "} "
        + "} ";
    rcaConf.readConfigFromString(configStr);
    dummyCache.put(node, ResourceUtil.HEAP_MAX_SIZE, heapMaxSizeInBytes);
    writeQueueStep =
        rcaConf.getQueueActionConfig().getStepSize(ResourceEnum.WRITE_THREADPOOL);
    searchQueueStep =
        rcaConf.getQueueActionConfig().getStepSize(ResourceEnum.SEARCH_THREADPOOL);
    fielddataCahceStepSize =
        rcaConf.getCacheActionConfig().getStepSize(ResourceEnum.FIELD_DATA_CACHE);
    shardRequestCacheStepSize =
        rcaConf.getCacheActionConfig().getStepSize(ResourceEnum.SHARD_REQUEST_CACHE);
    writeQueueStep = writeQueueStep * LevelTwoActionBuilderConfig.DEFAULT_WRITE_QUEUE_STEP_SIZE;
    searchQueueStep = searchQueueStep * LevelTwoActionBuilderConfig.DEFAULT_WRITE_QUEUE_STEP_SIZE;
    fielddataCahceStepSize = fielddataCahceStepSize * LevelTwoActionBuilderConfig.DEFAULT_FIELD_DATA_CACHE_STEP_SIZE;
    shardRequestCacheStepSize = shardRequestCacheStepSize * LevelTwoActionBuilderConfig.DEFAULT_FIELD_DATA_CACHE_STEP_SIZE;
  }

  @Test
  public void testDownSizeAllResources() {
    final double fielddataCacheSizeInPercent = 0.3;
    final double shardRequestCacheSizeInPercent = 0.04;
    final int writeQueueSize = 800;
    final int searchQueueSize = 2000;
    dummyCache.put(node, ResourceUtil.FIELD_DATA_CACHE_MAX_SIZE,
        (long) (heapMaxSizeInBytes * fielddataCacheSizeInPercent));
    dummyCache.put(node, ResourceUtil.SHARD_REQUEST_CACHE_MAX_SIZE,
        (long) (heapMaxSizeInBytes * shardRequestCacheSizeInPercent));
    dummyCache.put(node, ResourceUtil.WRITE_QUEUE_CAPACITY, writeQueueSize);
    dummyCache.put(node, ResourceUtil.SEARCH_QUEUE_CAPACITY, searchQueueSize);
    List<Action> actions = LevelTwoActionBuilder.newBuilder(node, testAppContext, rcaConf)
        .build();
    deciderActionParser.addActions(actions);
    Assert.assertEquals(4, deciderActionParser.size());
    int expectedQueueSize;
    ModifyQueueCapacityAction writeQueueAction = deciderActionParser.readQueueAction(ResourceEnum.WRITE_THREADPOOL);
    Assert.assertNotNull(writeQueueAction);
    expectedQueueSize = writeQueueSize - writeQueueStep;
    Assert.assertEquals(expectedQueueSize, writeQueueAction.getDesiredCapacity());
    Assert.assertEquals(writeQueueSize, writeQueueAction.getCurrentCapacity());

    ModifyQueueCapacityAction searchQueueAction = deciderActionParser.readQueueAction(ResourceEnum.SEARCH_THREADPOOL);
    Assert.assertNotNull(searchQueueAction);
    expectedQueueSize = searchQueueSize - searchQueueStep;
    Assert.assertEquals(expectedQueueSize, searchQueueAction.getDesiredCapacity());
    Assert.assertEquals(searchQueueSize, searchQueueAction.getCurrentCapacity());

    long expectedCacheSize;
    long currentCacheSize;
    ModifyCacheMaxSizeAction fielddataCacheAction = deciderActionParser.readCacheAction(ResourceEnum.FIELD_DATA_CACHE);
    Assert.assertNotNull(fielddataCacheAction);
    expectedCacheSize =
        (long) ((fielddataCacheSizeInPercent - fielddataCahceStepSize) * heapMaxSizeInBytes);
    currentCacheSize = (long) (fielddataCacheSizeInPercent * heapMaxSizeInBytes);
    Assert.assertEquals(expectedCacheSize, fielddataCacheAction.getDesiredCacheMaxSizeInBytes(), 10);
    Assert.assertEquals(currentCacheSize, fielddataCacheAction.getCurrentCacheMaxSizeInBytes(), 10);

    ModifyCacheMaxSizeAction requestCacheAction = deciderActionParser.readCacheAction(ResourceEnum.SHARD_REQUEST_CACHE);
    Assert.assertNotNull(requestCacheAction);
    expectedCacheSize =
        (long) ((shardRequestCacheSizeInPercent - shardRequestCacheStepSize) * heapMaxSizeInBytes);
    currentCacheSize = (long) (shardRequestCacheSizeInPercent * heapMaxSizeInBytes);
    Assert.assertEquals(expectedCacheSize, requestCacheAction.getDesiredCacheMaxSizeInBytes(), 10);
    Assert.assertEquals(currentCacheSize, requestCacheAction.getCurrentCacheMaxSizeInBytes(), 10);
  }

  @Test
  public void testDownSizeActionableResources() {
    final double fielddataCacheSizeInPercent = 0.3;
    final double shardRequestCacheSizeInPercent = CacheActionConfig.DEFAULT_SHARD_REQUEST_CACHE_LOWER_BOUND;
    final int writeQueueSize = QueueActionConfig.DEFAULT_WRITE_QUEUE_LOWER_BOUND;
    final int searchQueueSize = 2000;
    dummyCache.put(node, ResourceUtil.FIELD_DATA_CACHE_MAX_SIZE,
        (long) (heapMaxSizeInBytes * fielddataCacheSizeInPercent));
    dummyCache.put(node, ResourceUtil.SHARD_REQUEST_CACHE_MAX_SIZE,
        (long) (heapMaxSizeInBytes * shardRequestCacheSizeInPercent));
    dummyCache.put(node, ResourceUtil.WRITE_QUEUE_CAPACITY, writeQueueSize);
    dummyCache.put(node, ResourceUtil.SEARCH_QUEUE_CAPACITY, searchQueueSize);
    List<Action> actions = LevelTwoActionBuilder.newBuilder(node, testAppContext, rcaConf)
        .build();
    deciderActionParser.addActions(actions);

    Assert.assertEquals(2, deciderActionParser.size());

    int expectedQueueSize;
    ModifyQueueCapacityAction writeQueueAction = deciderActionParser.readQueueAction(ResourceEnum.WRITE_THREADPOOL);
    Assert.assertNull(writeQueueAction);

    ModifyQueueCapacityAction searchQueueAction = deciderActionParser.readQueueAction(ResourceEnum.SEARCH_THREADPOOL);
    Assert.assertNotNull(searchQueueAction);
    expectedQueueSize = searchQueueSize - searchQueueStep;
    Assert.assertEquals(expectedQueueSize, searchQueueAction.getDesiredCapacity());
    Assert.assertEquals(searchQueueSize, searchQueueAction.getCurrentCapacity());

    long expectedCacheSize;
    long currentCacheSize;
    ModifyCacheMaxSizeAction fielddataCacheAction = deciderActionParser.readCacheAction(ResourceEnum.FIELD_DATA_CACHE);
    Assert.assertNotNull(fielddataCacheAction);
    expectedCacheSize =
        (long) ((fielddataCacheSizeInPercent - fielddataCahceStepSize) * heapMaxSizeInBytes);
    currentCacheSize = (long) (fielddataCacheSizeInPercent * heapMaxSizeInBytes);
    Assert.assertEquals(expectedCacheSize, fielddataCacheAction.getDesiredCacheMaxSizeInBytes(), 10);
    Assert.assertEquals(currentCacheSize, fielddataCacheAction.getCurrentCacheMaxSizeInBytes(), 10);

    ModifyCacheMaxSizeAction requestCacheAction = deciderActionParser.readCacheAction(ResourceEnum.SHARD_REQUEST_CACHE);
    Assert.assertNull(requestCacheAction);
  }

  private void updateWorkLoadType(boolean preferIngest) throws Exception {
    final String configStr;
    if (preferIngest) {
      configStr =
        "{"
            + "\"decider-config-settings\": { "
              + "\"workload-type\": {"
                + "\"prefer-ingest\": true"
              + "}, "
              + "\"old-gen-decision-policy-config\": { "
                + "\"queue-bucket-size\": 20 "
              + "} "
            + "} "
        + "} ";
    } else {
       configStr =
        "{"
            + "\"decider-config-settings\": { "
              + "\"workload-type\": {"
                + "\"prefer-search\": true"
              + "}, "
              + "\"old-gen-decision-policy-config\": { "
                + "\"queue-bucket-size\": 20 "
              + "} "
            + "} "
        + "} ";
    }
    rcaConf.readConfigFromString(configStr);
  }

  @Test
  public void testSameBucketsAndPreferIngest() throws Exception {
    updateWorkLoadType(true);
    final double fielddataCacheSizeInPercent = CacheActionConfig.DEFAULT_FIELDDATA_CACHE_LOWER_BOUND;
    final double shardRequestCacheSizeInPercent = CacheActionConfig.DEFAULT_SHARD_REQUEST_CACHE_LOWER_BOUND;
    // bucket size for search queue = 500 / write queue = 190
    //bucket index = 2
    final int writeQueueSize = 155;
    //bucket index = 2
    final int searchQueueSize = 770;
    dummyCache.put(node, ResourceUtil.FIELD_DATA_CACHE_MAX_SIZE,
        (long) (heapMaxSizeInBytes * fielddataCacheSizeInPercent));
    dummyCache.put(node, ResourceUtil.SHARD_REQUEST_CACHE_MAX_SIZE,
        (long) (heapMaxSizeInBytes * shardRequestCacheSizeInPercent));
    dummyCache.put(node, ResourceUtil.WRITE_QUEUE_CAPACITY, writeQueueSize);
    dummyCache.put(node, ResourceUtil.SEARCH_QUEUE_CAPACITY, searchQueueSize);
    List<Action> actions = LevelTwoActionBuilder.newBuilder(node, testAppContext, rcaConf)
        .build();
    deciderActionParser.addActions(actions);

    Assert.assertEquals(1, deciderActionParser.size());
    ModifyCacheMaxSizeAction fielddataCacheAction = deciderActionParser.readCacheAction(ResourceEnum.FIELD_DATA_CACHE);
    Assert.assertNull(fielddataCacheAction);
    ModifyCacheMaxSizeAction requestCacheAction = deciderActionParser.readCacheAction(ResourceEnum.SHARD_REQUEST_CACHE);
    Assert.assertNull(requestCacheAction);
    ModifyQueueCapacityAction writeQueueAction = deciderActionParser.readQueueAction(ResourceEnum.WRITE_THREADPOOL);
    Assert.assertNull(writeQueueAction);

    ModifyQueueCapacityAction searchQueueAction = deciderActionParser.readQueueAction(ResourceEnum.SEARCH_THREADPOOL);
    Assert.assertNotNull(searchQueueAction);
    Assert.assertTrue(searchQueueAction.isActionable());
    int expectedQueueSize = searchQueueSize - searchQueueStep;
    Assert.assertEquals(expectedQueueSize, searchQueueAction.getDesiredCapacity());
    Assert.assertEquals(searchQueueSize, searchQueueAction.getCurrentCapacity());
  }

  @Test
  public void testSameBucketsAndPreferSearch() throws Exception {
    updateWorkLoadType(false);
    final double fielddataCacheSizeInPercent = CacheActionConfig.DEFAULT_FIELDDATA_CACHE_LOWER_BOUND;
    final double shardRequestCacheSizeInPercent = CacheActionConfig.DEFAULT_SHARD_REQUEST_CACHE_LOWER_BOUND;
    // bucket size for search queue = 500 / write queue = 190
    //bucket index = 0
    final int writeQueueSize = QueueActionConfig.DEFAULT_WRITE_QUEUE_LOWER_BOUND + 5;
    //bucket index = 0
    final int searchQueueSize = QueueActionConfig.DEFAULT_SEARCH_QUEUE_LOWER_BOUND + 20;
    dummyCache.put(node, ResourceUtil.FIELD_DATA_CACHE_MAX_SIZE,
        (long) (heapMaxSizeInBytes * fielddataCacheSizeInPercent));
    dummyCache.put(node, ResourceUtil.SHARD_REQUEST_CACHE_MAX_SIZE,
        (long) (heapMaxSizeInBytes * shardRequestCacheSizeInPercent));
    dummyCache.put(node, ResourceUtil.WRITE_QUEUE_CAPACITY, writeQueueSize);
    dummyCache.put(node, ResourceUtil.SEARCH_QUEUE_CAPACITY, searchQueueSize);
    List<Action> actions = LevelTwoActionBuilder.newBuilder(node, testAppContext, rcaConf)
        .build();
    deciderActionParser.addActions(actions);

    Assert.assertEquals(1, deciderActionParser.size());
    ModifyCacheMaxSizeAction fielddataCacheAction = deciderActionParser.readCacheAction(ResourceEnum.FIELD_DATA_CACHE);
    Assert.assertNull(fielddataCacheAction);
    ModifyCacheMaxSizeAction requestCacheAction = deciderActionParser.readCacheAction(ResourceEnum.SHARD_REQUEST_CACHE);
    Assert.assertNull(requestCacheAction);
    ModifyQueueCapacityAction writeQueueAction = deciderActionParser.readQueueAction(ResourceEnum.WRITE_THREADPOOL);
    Assert.assertNotNull(writeQueueAction);
    ModifyQueueCapacityAction searchQueueAction = deciderActionParser.readQueueAction(ResourceEnum.SEARCH_THREADPOOL);
    Assert.assertNull(searchQueueAction);
    Assert.assertTrue(writeQueueAction.isActionable());
    Assert.assertEquals(QueueActionConfig.DEFAULT_WRITE_QUEUE_LOWER_BOUND, writeQueueAction.getDesiredCapacity());
    Assert.assertEquals(writeQueueSize, writeQueueAction.getCurrentCapacity());
  }

  @Test
  public void testSearchQueueHasLargerBucketIndex() throws Exception {
    updateWorkLoadType(false);
    final double fielddataCacheSizeInPercent = CacheActionConfig.DEFAULT_FIELDDATA_CACHE_LOWER_BOUND;
    final double shardRequestCacheSizeInPercent = CacheActionConfig.DEFAULT_SHARD_REQUEST_CACHE_LOWER_BOUND;
    // bucket size for search queue = 500 / write queue = 190
    //bucket index = 1
    final int writeQueueSize = 280;
    //bucket index = 5
    final int searchQueueSize = 2100;
    dummyCache.put(node, ResourceUtil.FIELD_DATA_CACHE_MAX_SIZE,
        (long) (heapMaxSizeInBytes * fielddataCacheSizeInPercent));
    dummyCache.put(node, ResourceUtil.SHARD_REQUEST_CACHE_MAX_SIZE,
        (long) (heapMaxSizeInBytes * shardRequestCacheSizeInPercent));
    dummyCache.put(node, ResourceUtil.WRITE_QUEUE_CAPACITY, writeQueueSize);
    dummyCache.put(node, ResourceUtil.SEARCH_QUEUE_CAPACITY, searchQueueSize);
    List<Action> actions = LevelTwoActionBuilder.newBuilder(node, testAppContext, rcaConf)
        .build();
    deciderActionParser.addActions(actions);

    Assert.assertEquals(1, deciderActionParser.size());
    ModifyCacheMaxSizeAction fielddataCacheAction = deciderActionParser.readCacheAction(ResourceEnum.FIELD_DATA_CACHE);
    Assert.assertNull(fielddataCacheAction);
    ModifyCacheMaxSizeAction requestCacheAction = deciderActionParser.readCacheAction(ResourceEnum.SHARD_REQUEST_CACHE);
    Assert.assertNull(requestCacheAction);
    ModifyQueueCapacityAction writeQueueAction = deciderActionParser.readQueueAction(ResourceEnum.WRITE_THREADPOOL);
    Assert.assertNull(writeQueueAction);

    ModifyQueueCapacityAction searchQueueAction = deciderActionParser.readQueueAction(ResourceEnum.SEARCH_THREADPOOL);
    Assert.assertNotNull(searchQueueAction);
    Assert.assertTrue(searchQueueAction.isActionable());
    int expectedQueueSize = searchQueueSize - searchQueueStep;
    Assert.assertEquals(expectedQueueSize, searchQueueAction.getDesiredCapacity());
    Assert.assertEquals(searchQueueSize, searchQueueAction.getCurrentCapacity());
  }

  @Test
  public void testWriteQueueHasLargerBucketIndex() throws Exception {
    updateWorkLoadType(true);
    final double fielddataCacheSizeInPercent = CacheActionConfig.DEFAULT_FIELDDATA_CACHE_LOWER_BOUND;
    final double shardRequestCacheSizeInPercent = CacheActionConfig.DEFAULT_SHARD_REQUEST_CACHE_LOWER_BOUND;
    // bucket size for search queue = 500 / write queue = 190
    //bucket index = 4
    final int writeQueueSize = 690;
    //bucket index = 2
    final int searchQueueSize = 900;
    dummyCache.put(node, ResourceUtil.FIELD_DATA_CACHE_MAX_SIZE,
        (long) (heapMaxSizeInBytes * fielddataCacheSizeInPercent));
    dummyCache.put(node, ResourceUtil.SHARD_REQUEST_CACHE_MAX_SIZE,
        (long) (heapMaxSizeInBytes * shardRequestCacheSizeInPercent));
    dummyCache.put(node, ResourceUtil.WRITE_QUEUE_CAPACITY, writeQueueSize);
    dummyCache.put(node, ResourceUtil.SEARCH_QUEUE_CAPACITY, searchQueueSize);
    List<Action> actions = LevelTwoActionBuilder.newBuilder(node, testAppContext, rcaConf)
        .build();
    deciderActionParser.addActions(actions);

    Assert.assertEquals(1, deciderActionParser.size());
    ModifyCacheMaxSizeAction fielddataCacheAction = deciderActionParser.readCacheAction(ResourceEnum.FIELD_DATA_CACHE);
    Assert.assertNull(fielddataCacheAction);
    ModifyCacheMaxSizeAction requestCacheAction = deciderActionParser.readCacheAction(ResourceEnum.SHARD_REQUEST_CACHE);
    Assert.assertNull(requestCacheAction);
    ModifyQueueCapacityAction writeQueueAction = deciderActionParser.readQueueAction(ResourceEnum.WRITE_THREADPOOL);
    Assert.assertNotNull(writeQueueAction);
    ModifyQueueCapacityAction searchQueueAction = deciderActionParser.readQueueAction(ResourceEnum.SEARCH_THREADPOOL);
    Assert.assertNull(searchQueueAction);
    Assert.assertTrue(writeQueueAction.isActionable());
    int expectedQueueSize = writeQueueSize - writeQueueStep;
    Assert.assertEquals(expectedQueueSize, writeQueueAction.getDesiredCapacity());
    Assert.assertEquals(writeQueueSize, writeQueueAction.getCurrentCapacity());
  }
}
