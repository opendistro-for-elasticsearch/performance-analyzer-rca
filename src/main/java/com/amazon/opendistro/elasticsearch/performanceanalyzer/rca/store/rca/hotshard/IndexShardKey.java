/*
 * Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.hotshard;

public class IndexShardKey {

    private final String indexName;
    private final String shardId;

    public IndexShardKey(String indexName, String shardId) {
        this.indexName = indexName;
        this.shardId = shardId;
    }

    public String getIndexName() {
        return this.indexName;
    }

    public String getShardId() {
        return this.shardId;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj != null && obj instanceof IndexShardKey) {
            IndexShardKey key = (IndexShardKey)obj;
            return indexName.equals(key.indexName) && shardId.equals(key.shardId);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return indexName.hashCode() * 31 + shardId.hashCode();
    }

    @Override
    public String toString() {
        return String.join(" ", new String[] { this.indexName, this.shardId});
    }
}
