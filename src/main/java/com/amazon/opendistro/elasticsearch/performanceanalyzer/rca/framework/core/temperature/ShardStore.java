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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.temperature;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.temperature.ShardProfileSummary;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nonnull;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * The same set of shards will be seen for each metric and across multiple operations.
 * This creates a pool of all shards so that they can be referenced from multiple places.
 */
public class ShardStore {
    private static final Logger LOG = LogManager.getLogger(ShardStore.class);

    /**
     * The key for the outer map is indexName. The key for inner map is the ShardID. Given an
     * indexName and shardId, a shard can be uniquely identified.
     *
     * <p>TODO: Try replace with 'IndexShardKey' after
     * https://github.com/opendistro-for-elasticsearch/performance-analyzer-rca/pull/141
     * is merged.
     */
    Map<String, Map<Integer, ShardProfileSummary>> list;

    public ShardStore() {
        // ShardStore is modified by all the RcaGraph nodes that calculate temperature along a
        // dimension. As these nodes are in the same level of the RCA DAG, different threads can
        // execute them and hence we need this map to be synchronized.
        list = new ConcurrentHashMap<>();
    }

    @Nonnull
    public synchronized ShardProfileSummary getOrCreateIfAbsent(String indexName, int shardId) {
        Map<Integer, ShardProfileSummary> innerMap = list.get(indexName);
        if (innerMap == null) {
            // No element with the index name exists; create one.
            innerMap = new ConcurrentHashMap<>();
            list.put(indexName, innerMap);
        }
        ShardProfileSummary shardProfileSummary = innerMap.get(shardId);
        if (shardProfileSummary == null) {
            // Could not find a shard with the given indexname and shardId; create one.
            shardProfileSummary = new ShardProfileSummary(indexName, shardId);
            innerMap.put(shardId, shardProfileSummary);
        }
        return shardProfileSummary;
    }

    public List<ShardProfileSummary> getAllShards() {
        List<ShardProfileSummary> shardProfileSummaryList = new ArrayList<>();
        for (Map<Integer, ShardProfileSummary> shardIdToShardMap : list.values()) {
            shardProfileSummaryList.addAll(shardIdToShardMap.values());
        }
        return shardProfileSummaryList;
    }
}
