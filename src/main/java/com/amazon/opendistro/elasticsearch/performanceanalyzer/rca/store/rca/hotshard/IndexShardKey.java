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

import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.CommonDimension;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.Record;
import org.jooq.exception.DataTypeException;

public class IndexShardKey {

  private static final Logger LOG = LogManager.getLogger(IndexShardKey.class);
  private final String indexName;
  private final int shardId;

  public IndexShardKey(String indexName, int shardId) {
    this.indexName = indexName;
    this.shardId = shardId;
  }

  public String getIndexName() {
    return this.indexName;
  }

  public int getShardId() {
    return this.shardId;
  }

  public static IndexShardKey buildIndexShardKey(Record record) throws IllegalArgumentException {
    if (record == null) {
      throw new IllegalArgumentException("record is null");
    }
    try {
      String indexName = record.getValue(CommonDimension.INDEX_NAME.toString(), String.class);
      Integer shardId = record.getValue(CommonDimension.SHARD_ID.toString(), Integer.class);
      return new IndexShardKey(indexName, shardId);
    }
    catch (DataTypeException de) {
      LOG.error("Fail to read field from SQL record, message {}", de.getMessage());
      throw new IllegalArgumentException("failed to read field from record");
    }
  }

  @Override
  public boolean equals(Object obj) {
    if (obj != null && obj instanceof IndexShardKey) {
      IndexShardKey key = (IndexShardKey)obj;
      return indexName.equals(key.indexName) && shardId == key.shardId;
    }
    return false;
  }

  @Override
  public int hashCode() {
    return indexName.hashCode() * 31 + shardId;
  }

  @Override
  public String toString() {
    return "[" + this.indexName + "][" + this.shardId + "]";
  }
}
