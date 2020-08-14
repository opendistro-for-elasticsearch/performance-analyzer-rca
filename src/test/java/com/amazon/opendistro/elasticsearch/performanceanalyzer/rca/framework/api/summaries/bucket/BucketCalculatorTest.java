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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.bucket;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.ResourceEnum;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.util.InstanceDetails.Id;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.util.InstanceDetails.Ip;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.cluster.NodeKey;
import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.List;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public abstract class BucketCalculatorTest {
  protected static final double TEST_VALUE = 50.0;
  protected static List<ResourceEnum> resources;
  protected static List<NodeKey> nodes;
  protected BucketCalculator uut;

  protected static NodeKey genNodeKey(int id) {
    return new NodeKey(new Id(Integer.toBinaryString(id)), new Ip("127.0.0.1"));
  }

  protected static void initClass() {
    resources = Lists.newArrayList(ResourceEnum.values());
    nodes = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      nodes.add(genNodeKey(i));
    }
  }

  @BeforeClass
  public static void setupClass() {
    initClass();
  }

  abstract void initUut();

  abstract void verifyCompute();

  @Before
  public void setup() {
    initUut();
  }

  @Test
  public void testCompute() {
    verifyCompute();
  }

  @Test
  public void testBasicReadWrite() {
    for (NodeKey nodeKey : nodes) {
      for (ResourceEnum resourceEnum : resources) {
        uut.computeBucket(nodeKey, resourceEnum, TEST_VALUE);
        Assert.assertEquals(uut.compute(resourceEnum, TEST_VALUE), uut.getUsageBucket(nodeKey, resourceEnum));
      }
    }
  }
}
