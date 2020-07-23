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

import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.Resource;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.cluster.NodeKey;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.builder.HashCodeBuilder;

/**
 * we create a thread-safe unbounded cache instance in {@link com.amazon.opendistro.elasticsearch.performanceanalyzer.AppContext}
 * to store the node config settings from each node. Any RCA vertex in RCA graph can read the node config directly from
 * this cache instance. The key of this cache is NodeKey + Resource and value is the actual value of the config setting
 * (i.e. size of write queue capacity)
 */
public class NodeConfigCache {

  private static final int CACHE_TTL = 10;
  private final Cache<NodeConfigKey, Double> nodeConfigCache;

  //unbounded cache with TTL set to 10 mins
  public NodeConfigCache() {
    nodeConfigCache =
        CacheBuilder.newBuilder()
            .expireAfterWrite(CACHE_TTL, TimeUnit.MINUTES)
            .build();
  }

  /**
   * add config value into cache
   * @param nodeKey the NodeKey of the node on which this config is collected
   * @param config the config type
   * @param value the config value
   */
  public void put(NodeKey nodeKey, Resource config, double value) {
    nodeConfigCache.put(new NodeConfigKey(nodeKey, config), value);
  }

  /**
   * returns the config value that is associated with the nodeKey and config
   * @param nodeKey the NodeKey of the node
   * @param config the config type
   * @return the config value
   * @throws IllegalArgumentException throws an exception if the config does not exist in cache
   */
  public double get(NodeKey nodeKey, Resource config) throws IllegalArgumentException {
    Double ret = nodeConfigCache.getIfPresent(new NodeConfigKey(nodeKey, config));
    if (ret == null) {
      throw new IllegalArgumentException();
    }
    return ret;
  }

  private static class NodeConfigKey {
    private final NodeKey nodeKey;
    private final Resource resource;

    public NodeConfigKey(final NodeKey nodeKey, final Resource resource) {
      this.nodeKey = nodeKey;
      this.resource = resource;
    }

    public NodeKey getNodeKey() {
      return this.nodeKey;
    }

    public Resource getResource() {
      return this.resource;
    }

    @Override
    public boolean equals(Object obj) {
      if (obj instanceof NodeConfigKey) {
        NodeConfigKey key = (NodeConfigKey)obj;
        return nodeKey.equals(key.getNodeKey()) && resource.equals(key.getResource());
      }
      return false;
    }

    @Override
    public int hashCode() {
      return new HashCodeBuilder(17, 37)
          .append(nodeKey.hashCode())
          .append(resource.hashCode())
          .toHashCode();
    }
  }
}
