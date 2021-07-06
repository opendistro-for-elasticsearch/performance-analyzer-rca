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
import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.CacheClearAction;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.ModifyCacheMaxSizeAction;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.ModifyQueueCapacityAction;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.configs.CacheActionConfig;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.configs.QueueActionConfig;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.deciders.configs.jvm.LevelThreeActionBuilderConfig;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.deciders.jvm.old_gen.LevelOneActionBuilder;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.deciders.jvm.old_gen.LevelThreeActionBuilder;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.deciders.test_utils.DeciderActionParserUtil;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.ResourceEnum;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.NodeRole;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.ResourceUtil;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.RcaConf;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.util.InstanceDetails;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.collector.NodeConfigCache;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.cluster.NodeKey;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.reader.ClusterDetailsEventProcessor;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.reader.ClusterDetailsEventProcessor.NodeDetails;
import java.util.ArrayList;
import java.util.List;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class LevelThreeActionBuilderTest {

  private AppContext testAppContext;
  private NodeConfigCache dummyCache;
  private RcaConf rcaConf;
  private NodeKey node;
  private final long heapMaxSizeInBytes = 10 * GB_TO_BYTES;
  private int writeQueueStep;
  private int searchQueueStep;
  private DeciderActionParserUtil deciderActionParser;

  public LevelThreeActionBuilderTest() {
    testAppContext = new AppContext();
    dummyCache = testAppContext.getNodeConfigCache();
    rcaConf = new RcaConf();
    node = new NodeKey(new InstanceDetails.Id("node-1"), new InstanceDetails.Ip("127.0.0.1"));
    deciderActionParser = new DeciderActionParserUtil();
  }

  private void setupClusterDetails() {
    ClusterDetailsEventProcessor clusterDetailsEventProcessor = new ClusterDetailsEventProcessor();
    ClusterDetailsEventProcessor.NodeDetails node1 =
        new ClusterDetailsEventProcessor.NodeDetails(NodeRole.DATA, "node1", "127.0.0.0", false);
    ClusterDetailsEventProcessor.NodeDetails node2 =
        new ClusterDetailsEventProcessor.NodeDetails(NodeRole.DATA, "node2", "127.0.0.1", false);
    ClusterDetailsEventProcessor.NodeDetails master =
        new ClusterDetailsEventProcessor.NodeDetails(NodeRole.ELECTED_MASTER, "master", "127.0.0.3",
            true);

    List<NodeDetails> nodes = new ArrayList<>();
    nodes.add(node1);
    nodes.add(node2);
    nodes.add(master);
    clusterDetailsEventProcessor.setNodesDetails(nodes);
    testAppContext.setClusterDetailsEventProcessor(clusterDetailsEventProcessor);
  }

  @Before
  public void init() throws Exception {
    final String configStr =
        "{"
            + "\"decider-config-settings\": { "
              + "\"old-gen-decision-policy-config\": { "
                + "\"queue-bucket-size\": 10 "
              + "} "
            + "} "
        + "} ";
    rcaConf.readConfigFromString(configStr);
    dummyCache.put(node, ResourceUtil.HEAP_MAX_SIZE, heapMaxSizeInBytes);
    writeQueueStep =
        rcaConf.getQueueActionConfig().getStepSize(ResourceEnum.WRITE_THREADPOOL);
    searchQueueStep =
        rcaConf.getQueueActionConfig().getStepSize(ResourceEnum.SEARCH_THREADPOOL);
    writeQueueStep = writeQueueStep * LevelThreeActionBuilderConfig.DEFAULT_WRITE_QUEUE_STEP_SIZE;
    searchQueueStep = searchQueueStep * LevelThreeActionBuilderConfig.DEFAULT_WRITE_QUEUE_STEP_SIZE;
    setupClusterDetails();
  }

  @Test
  public void testDownSizeAllResources() {
    final double fielddataCacheSizeInPercent = 0.3;
    final double shardRequestCacheSizeInPercent = 0.04;
    final int writeQueueSize = 800;
    final int searchQueueSize = 2000;
    dummyCache.put(node, ResourceUtil.FIELD_DATA_CACHE_MAX_SIZE,
        (long)(heapMaxSizeInBytes * fielddataCacheSizeInPercent));
    dummyCache.put(node, ResourceUtil.SHARD_REQUEST_CACHE_MAX_SIZE,
        (long)(heapMaxSizeInBytes * shardRequestCacheSizeInPercent));
    dummyCache.put(node, ResourceUtil.WRITE_QUEUE_CAPACITY, writeQueueSize);
    dummyCache.put(node, ResourceUtil.SEARCH_QUEUE_CAPACITY, searchQueueSize);
    List<Action> actions = LevelThreeActionBuilder.newBuilder(node, testAppContext, rcaConf).build();
    deciderActionParser.addActions(actions);

    Assert.assertEquals(5, deciderActionParser.size());

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
    expectedCacheSize = (long) (CacheActionConfig.DEFAULT_FIELDDATA_CACHE_LOWER_BOUND * heapMaxSizeInBytes);
    currentCacheSize = (long) (fielddataCacheSizeInPercent * heapMaxSizeInBytes);
    Assert.assertEquals(expectedCacheSize, fielddataCacheAction.getDesiredCacheMaxSizeInBytes(), 10);
    Assert.assertEquals(currentCacheSize, fielddataCacheAction.getCurrentCacheMaxSizeInBytes(), 10);

    ModifyCacheMaxSizeAction requestCacheAction = deciderActionParser.readCacheAction(ResourceEnum.SHARD_REQUEST_CACHE);
    Assert.assertNotNull(requestCacheAction);
    expectedCacheSize = (long) (CacheActionConfig.DEFAULT_SHARD_REQUEST_CACHE_LOWER_BOUND * heapMaxSizeInBytes);
    currentCacheSize = (long) (shardRequestCacheSizeInPercent * heapMaxSizeInBytes);
    Assert.assertEquals(expectedCacheSize, requestCacheAction.getDesiredCacheMaxSizeInBytes(), 10);
    Assert.assertEquals(currentCacheSize, requestCacheAction.getCurrentCacheMaxSizeInBytes(), 10);

    CacheClearAction cacheClearAction = deciderActionParser.readCacheClearAction();
    Assert.assertNotNull(cacheClearAction);
    Assert.assertTrue(cacheClearAction.isActionable());
    Assert.assertEquals(2, cacheClearAction.impactedNodes().size());
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
    List<Action> actions = LevelThreeActionBuilder.newBuilder(node, testAppContext, rcaConf)
        .build();
    deciderActionParser.addActions(actions);

    Assert.assertEquals(3, deciderActionParser.size());

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
    expectedCacheSize = (long) (CacheActionConfig.DEFAULT_FIELDDATA_CACHE_LOWER_BOUND * heapMaxSizeInBytes);
    currentCacheSize = (long) (fielddataCacheSizeInPercent * heapMaxSizeInBytes);
    Assert.assertEquals(expectedCacheSize, fielddataCacheAction.getDesiredCacheMaxSizeInBytes(), 10);
    Assert.assertEquals(currentCacheSize, fielddataCacheAction.getCurrentCacheMaxSizeInBytes(), 10);

    ModifyCacheMaxSizeAction requestCacheAction = deciderActionParser.readCacheAction(ResourceEnum.SHARD_REQUEST_CACHE);
    Assert.assertNull(requestCacheAction);

    CacheClearAction cacheClearAction = deciderActionParser.readCacheClearAction();
    Assert.assertNotNull(cacheClearAction);
    Assert.assertTrue(cacheClearAction.isActionable());
    Assert.assertEquals(2, cacheClearAction.impactedNodes().size());
  }

  @Test
  public void testSuppressActionWhenCacheUsageIsLow() {
    final double fielddataCacheSizeInPercent = 0.3;
    final double shardRequestCacheSizeInPercent = 0.04;
    final int writeQueueSize = QueueActionConfig.DEFAULT_WRITE_QUEUE_LOWER_BOUND;
    final int searchQueueSize = 2000;
    dummyCache.put(node, ResourceUtil.FIELD_DATA_CACHE_MAX_SIZE,
        (long)(heapMaxSizeInBytes * fielddataCacheSizeInPercent));
    dummyCache.put(node, ResourceUtil.SHARD_REQUEST_CACHE_MAX_SIZE,
        (long)(heapMaxSizeInBytes * shardRequestCacheSizeInPercent));
    final double fielddataCacheUsageInPercent = 0.02;
    final double shardRequestCacheUsageInPercent = 0.02;
    dummyCache.put(node, ResourceUtil.FIELD_DATA_CACHE_ACTUAL_SIZE,
        (long)(heapMaxSizeInBytes * fielddataCacheUsageInPercent));
    dummyCache.put(node, ResourceUtil.SHARD_REQUEST_CACHE_ACTUAL_SIZE,
        (long)(heapMaxSizeInBytes * shardRequestCacheUsageInPercent));
    dummyCache.put(node, ResourceUtil.WRITE_QUEUE_CAPACITY, writeQueueSize);
    dummyCache.put(node, ResourceUtil.SEARCH_QUEUE_CAPACITY, searchQueueSize);
    List<Action> actions = LevelThreeActionBuilder.newBuilder(node, testAppContext, rcaConf).build();
    deciderActionParser.addActions(actions);

    Assert.assertEquals(3, actions.size());
    long expectedCacheSize;
    long currentCacheSize;
    ModifyCacheMaxSizeAction requestCacheAction = deciderActionParser.readCacheAction(ResourceEnum.SHARD_REQUEST_CACHE);
    Assert.assertNotNull(requestCacheAction);
    expectedCacheSize = (long) (CacheActionConfig.DEFAULT_SHARD_REQUEST_CACHE_LOWER_BOUND * heapMaxSizeInBytes);
    currentCacheSize = (long) (shardRequestCacheSizeInPercent * heapMaxSizeInBytes);
    Assert.assertEquals(expectedCacheSize, requestCacheAction.getDesiredCacheMaxSizeInBytes(), 10);
    Assert.assertEquals(currentCacheSize, requestCacheAction.getCurrentCacheMaxSizeInBytes(), 10);
    ModifyCacheMaxSizeAction fielddatatCacheAction = deciderActionParser.readCacheAction(ResourceEnum.FIELD_DATA_CACHE);
    Assert.assertNull(fielddatatCacheAction);
  }
}
