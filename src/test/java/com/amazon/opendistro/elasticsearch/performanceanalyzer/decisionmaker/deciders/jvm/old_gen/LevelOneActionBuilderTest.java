package com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.deciders.jvm.old_gen;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.Action;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.ModifyCacheMaxSizeAction;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.ResourceEnum;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.ResourceUtil;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.util.InstanceDetails.Id;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.util.InstanceDetails.Ip;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.collector.NodeConfigCache;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.cluster.NodeKey;
import java.util.List;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class LevelOneActionBuilderTest {
  private static long GB_TO_BYTE = 1024 * 1024 * 1024;
  private static long MB_TO_BYTE = 1024 * 1024;
  //heap size is 10GB
  private static double HEAP_SIZE = 10 * (double) GB_TO_BYTE;
  private NodeConfigCache dummyCache;
  private NodeKey node;

  @Before
  public void init() {
    dummyCache = new NodeConfigCache();
    node = new NodeKey(new Id("node1"), new Ip("127.0.0.1"));
    dummyCache.put(node, ResourceUtil.SEARCH_QUEUE_CAPACITY, 1000);
    dummyCache.put(node, ResourceUtil.WRITE_QUEUE_CAPACITY, 100);
    dummyCache.put(node, ResourceUtil.HEAP_MAX_SIZE, HEAP_SIZE);
  }

  @Test
  public void testDownGradingAllCaches() {
    dummyCache.put(node, ResourceUtil.FIELD_DATA_CACHE_MAX_SIZE, 0.2 * HEAP_SIZE);
    dummyCache.put(node, ResourceUtil.SHARD_REQUEST_CACHE_MAX_SIZE, 0.04 * HEAP_SIZE);
    List<Action> actions = LevelOneActionBuilder.newBuilder(node, dummyCache).build();
    Assert.assertNotNull(actions);
    Assert.assertEquals(2, actions.size());
    Assert.assertEquals(ModifyCacheMaxSizeAction.NAME, actions.get(0).name());
    Assert.assertEquals(ModifyCacheMaxSizeAction.NAME, actions.get(1).name());
    ModifyCacheMaxSizeAction cacheAction1 = (ModifyCacheMaxSizeAction)actions.get(0);
    ModifyCacheMaxSizeAction cacheAction2 = (ModifyCacheMaxSizeAction)actions.get(1);
    Assert.assertTrue(cacheAction1.isActionable());
    Assert.assertTrue(cacheAction2.isActionable());
    if (cacheAction1.getCacheType() == ResourceEnum.SHARD_REQUEST_CACHE) {
      Assert.assertEquals(ResourceEnum.SHARD_REQUEST_CACHE, cacheAction1.getCacheType());
      Assert.assertEquals(ResourceEnum.FIELD_DATA_CACHE, cacheAction2.getCacheType());
    }
    else {
      Assert.assertEquals(ResourceEnum.FIELD_DATA_CACHE, cacheAction1.getCacheType());
      Assert.assertEquals(ResourceEnum.SHARD_REQUEST_CACHE, cacheAction2.getCacheType());
    }
  }

  @Test
  public void testDownGradingOneCache() {
    dummyCache.put(node, ResourceUtil.FIELD_DATA_CACHE_MAX_SIZE, 0.1 * HEAP_SIZE + 500 * MB_TO_BYTE);
    dummyCache.put(node, ResourceUtil.SHARD_REQUEST_CACHE_MAX_SIZE, 0.04 * HEAP_SIZE);
    List<Action> actions = LevelOneActionBuilder.newBuilder(node, dummyCache).build();
    Assert.assertNotNull(actions);
    Assert.assertEquals(1, actions.size());
    Assert.assertEquals(ModifyCacheMaxSizeAction.NAME, actions.get(0).name());
    ModifyCacheMaxSizeAction cacheAction1 = (ModifyCacheMaxSizeAction)actions.get(0);
    Assert.assertTrue(cacheAction1.isActionable());
    Assert.assertEquals(ResourceEnum.SHARD_REQUEST_CACHE, cacheAction1.getCacheType());
  }

  @Test
  public void testNoAvailableAction() {
    dummyCache.put(node, ResourceUtil.FIELD_DATA_CACHE_MAX_SIZE, 0.09 * HEAP_SIZE);
    dummyCache.put(node, ResourceUtil.SHARD_REQUEST_CACHE_MAX_SIZE, 0.019 * HEAP_SIZE);
    List<Action> actions = LevelOneActionBuilder.newBuilder(node, dummyCache).build();
    Assert.assertNotNull(actions);
    Assert.assertEquals(0, actions.size());
  }
}
