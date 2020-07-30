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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.collector;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.MetricEnum;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.Resource;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.ResourceEnum;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.cluster.NodeKey;

public class NodeConfigCacheUtil {

  public static Long readCacheSize(NodeKey esNode, NodeConfigCache nodeConfigCache, ResourceEnum resourceEnum) {
    Long ret;
    Resource resource = Resource.newBuilder()
        .setResourceEnum(resourceEnum)
        .setMetricEnum(MetricEnum.CACHE_MAX_SIZE).build();
    try {
      ret = (long) nodeConfigCache.get(esNode, resource);
    } catch (Exception e) {
      ret = null;
    }
    return ret;
  }

  public static Integer readQueueCapacity(NodeKey esNode, NodeConfigCache nodeConfigCache, ResourceEnum resourceEnum) {
    Integer ret;
    Resource resource = Resource.newBuilder()
        .setResourceEnum(resourceEnum)
        .setMetricEnum(MetricEnum.QUEUE_CAPACITY).build();
    try {
      ret = (int) nodeConfigCache.get(esNode, resource);
    } catch (Exception e) {
      ret = null;
    }
    return ret;
  }

  //TODO : add RCA to read queue size(in EWMA)
  public static Integer readQueueSize(NodeKey esNode, NodeConfigCache nodeConfigCache, ResourceEnum resourceEnum) {
    return 100;
  }
}
