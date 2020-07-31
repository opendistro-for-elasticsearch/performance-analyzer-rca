package com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.util;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.ResourceEnum;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.ResourceUtil;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.collector.NodeConfigCache;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.cluster.NodeKey;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class NodeConfigReaderUtil {
  private static final Logger LOG = LogManager.getLogger(NodeConfigReaderUtil.class);

  public static Long getCacheMaxSizeInBytes(
      final NodeConfigCache nodeConfigCache, final NodeKey esNode, final ResourceEnum cacheType) {
    try {
      if (cacheType.equals(ResourceEnum.FIELD_DATA_CACHE)) {
        return (long) nodeConfigCache.get(esNode, ResourceUtil.FIELD_DATA_CACHE_MAX_SIZE);
      }
      return (long) nodeConfigCache.get(esNode, ResourceUtil.SHARD_REQUEST_CACHE_MAX_SIZE);
    } catch (final IllegalArgumentException e) {
      LOG.error("Exception while reading cache max size from Node Config Cache", e);
    }
    // No action if value not present in the cache.
    // No action will be triggered as this value was wiped out from the cache
    return null;
  }

  public static Long getHeapMaxSizeInBytes(final NodeConfigCache nodeConfigCache, final NodeKey esNode) {
    try {
      return (long) nodeConfigCache.get(esNode, ResourceUtil.HEAP_MAX_SIZE);
    } catch (final IllegalArgumentException e) {
      LOG.error("Exception while reading heap max size from Node Config Cache", e);
    }
    // No action if value not present in the cache.
    // No action will be triggered as this value was wiped out from the cache
    return null;
  }
}
