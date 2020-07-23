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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.store.collector;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.GradleTaskForRca;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.ResourceUtil;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.collector.NodeConfigCache;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.cluster.NodeKey;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(GradleTaskForRca.class)
public class NodeConfigCacheTest {

  private NodeConfigCache nodeConfigCache;
  private NodeKey nodeKey1;
  private NodeKey nodeKey2;

  @Before
  public void init() {
    this.nodeConfigCache = new NodeConfigCache();
    this.nodeKey1 = new NodeKey("node1", "127.0.0.1");
    this.nodeKey2 = new NodeKey("node2", "127.0.0.2");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testNonExistentKey() {
    double val = nodeConfigCache.get(nodeKey1, ResourceUtil.WRITE_QUEUE_CAPACITY);
    Assert.fail();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testReadWrongKey() {
    nodeConfigCache.put(nodeKey1, ResourceUtil.WRITE_QUEUE_CAPACITY, 2.0);
    double val = nodeConfigCache.get(nodeKey1, ResourceUtil.WRITE_QUEUE_REJECTION);
    Assert.fail();
  }

  @Test
  public void testSetAndGetValue() {
    nodeConfigCache.put(nodeKey1, ResourceUtil.WRITE_QUEUE_CAPACITY, 3.0);
    double val = nodeConfigCache.get(nodeKey1, ResourceUtil.WRITE_QUEUE_CAPACITY);
    Assert.assertEquals(3.0, val, 0.01);

    nodeConfigCache.put(nodeKey1, ResourceUtil.WRITE_QUEUE_CAPACITY, 4.0);
    val = nodeConfigCache.get(nodeKey1, ResourceUtil.WRITE_QUEUE_CAPACITY);
    Assert.assertEquals(4.0, val, 0.01);
  }
}
