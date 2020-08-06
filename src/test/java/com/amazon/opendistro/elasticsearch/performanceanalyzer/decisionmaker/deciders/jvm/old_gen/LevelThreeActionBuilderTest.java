package com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.deciders.jvm.old_gen;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.Action;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.ModifyCacheMaxSizeAction;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.ModifyQueueCapacityAction;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.deciders.jvm.ActionTestUtil;
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

public class LevelThreeActionBuilderTest {
  public static long GB_TO_BYTE = 1024 * 1024 * 1024;
  public static long MB_TO_BYTE = 1024 * 1024;
  //heap size is 10GB
  private static double HEAP_SIZE = 10 * (double) GB_TO_BYTE;
  private NodeConfigCache dummyCache;
  private NodeKey node;

  @Before
  public void init() {
    dummyCache = new NodeConfigCache();
    node = new NodeKey(new Id("node1"), new Ip("127.0.0.1"));
    dummyCache.put(node, ResourceUtil.SEARCH_QUEUE_CAPACITY, 2000);
    dummyCache.put(node, ResourceUtil.WRITE_QUEUE_CAPACITY, 500);
    dummyCache.put(node, ResourceUtil.HEAP_MAX_SIZE, HEAP_SIZE);
  }

  @Test
  public void testDownGradingAllCachesAndQueue() {
    dummyCache.put(node, ResourceUtil.FIELD_DATA_CACHE_MAX_SIZE, 0.05 * HEAP_SIZE);
    dummyCache.put(node, ResourceUtil.SHARD_REQUEST_CACHE_MAX_SIZE, 0.01 * HEAP_SIZE);
    List<Action> actions = LevelThreeActionBuilder.newBuilder(node, dummyCache).build();
    Assert.assertNotNull(actions);
    Assert.assertEquals(4, actions.size());
    ActionTestUtil actionTestUtil = new ActionTestUtil(actions);

    ModifyQueueCapacityAction writeQueueAction =
        actionTestUtil.readAction(ResourceEnum.WRITE_THREADPOOL, ModifyQueueCapacityAction.class);
    Assert.assertNotNull(writeQueueAction);
    Assert.assertTrue(writeQueueAction.isActionable());
    Assert.assertEquals(2*50, writeQueueAction.getCurrentCapacity() - writeQueueAction.getDesiredCapacity());

    ModifyQueueCapacityAction searchQueueAction =
        actionTestUtil.readAction(ResourceEnum.SEARCH_THREADPOOL, ModifyQueueCapacityAction.class);
    Assert.assertNotNull(searchQueueAction);
    Assert.assertTrue(searchQueueAction.isActionable());
    Assert.assertEquals(2*50, searchQueueAction.getCurrentCapacity() - searchQueueAction.getDesiredCapacity());

    ModifyCacheMaxSizeAction fieldDataAction =
        actionTestUtil.readAction(ResourceEnum.FIELD_DATA_CACHE, ModifyCacheMaxSizeAction.class);
    Assert.assertNotNull(fieldDataAction);
    Assert.assertTrue(fieldDataAction.isActionable());
    Assert.assertEquals(0, fieldDataAction.getDesiredCacheMaxSizeInBytes().longValue());

    ModifyCacheMaxSizeAction shardRequestAction =
        actionTestUtil.readAction(ResourceEnum.SHARD_REQUEST_CACHE, ModifyCacheMaxSizeAction.class);
    Assert.assertNotNull(shardRequestAction);
    Assert.assertTrue(shardRequestAction.isActionable());
    Assert.assertEquals(0, shardRequestAction.getDesiredCacheMaxSizeInBytes().longValue());
  }
}
