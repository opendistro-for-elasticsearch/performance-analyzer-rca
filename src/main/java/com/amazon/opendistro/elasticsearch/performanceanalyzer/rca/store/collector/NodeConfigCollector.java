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

import static com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.CacheConfigDimension.CACHE_TYPE;
import static com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.ThreadPoolDimension.THREAD_POOL_TYPE;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.Resource;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.CacheType;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.ThreadPoolType;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metricsdb.MetricsDB;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.EsConfigNode;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.flow_units.MetricFlowUnit;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.flow_units.NodeConfigFlowUnit;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics.Cache_Max_Size;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics.ThreadPool_QueueCapacity;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.persist.SQLParsingUtil;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.ResourceUtil;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.cluster.NodeKey;
import java.util.HashMap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * This is a node level collector in RCA graph which collect the current config settings from ES (queue/cache capacity etc.)
 * And pass them down to Decision Maker for the next round of resource auto-tuning.
 */
public class NodeConfigCollector extends EsConfigNode {

  private static final Logger LOG = LogManager.getLogger(NodeConfigCollector.class);
  private final ThreadPool_QueueCapacity threadPool_queueCapacity;
  private final Cache_Max_Size cacheMaxSize;
  private final int rcaPeriod;
  private int counter;
  private final HashMap<Resource, Double> configResult;

  public NodeConfigCollector(int rcaPeriod,
                             ThreadPool_QueueCapacity threadPool_queueCapacity,
                             Cache_Max_Size cacheMaxSize) {
    this.threadPool_queueCapacity = threadPool_queueCapacity;
    this.cacheMaxSize = cacheMaxSize;
    this.rcaPeriod = rcaPeriod;
    this.counter = 0;
    this.configResult = new HashMap<>();
  }

  private void collectQueueCapacity(MetricFlowUnit flowUnit) {
    double writeQueueCapacity = SQLParsingUtil.readDataFromSqlResult(flowUnit.getData(),
        THREAD_POOL_TYPE.getField(), ThreadPoolType.WRITE.toString(), MetricsDB.MAX);
    if (!Double.isNaN(writeQueueCapacity)) {
      configResult.put(ResourceUtil.WRITE_QUEUE_CAPACITY, writeQueueCapacity);
    }
    else {
      LOG.error("write queue capacity is NaN");
    }
    double searchQueueCapacity = SQLParsingUtil.readDataFromSqlResult(flowUnit.getData(),
        THREAD_POOL_TYPE.getField(), ThreadPoolType.SEARCH.toString(), MetricsDB.MAX);
    if (!Double.isNaN(searchQueueCapacity)) {
      configResult.put(ResourceUtil.SEARCH_QUEUE_CAPACITY, searchQueueCapacity);
    }
    else {
      LOG.error("search queue capacity is NaN");
    }
  }

  private void collectCacheMaxSize(MetricFlowUnit cacheMaxSize) {
    double fieldDataCacheMaxSize = SQLParsingUtil.readDataFromSqlResult(cacheMaxSize.getData(),
            CACHE_TYPE.getField(), CacheType.FIELD_DATA_CACHE.toString(), MetricsDB.MAX);
    if (!Double.isNaN(fieldDataCacheMaxSize)) {
      configResult.put(ResourceUtil.FIELD_DATA_CACHE_MAX_SIZE, fieldDataCacheMaxSize);
    }
    else {
      LOG.error("Field Data cache max size is NaN");
    }

    double shardRequestCacheMaxSize = SQLParsingUtil.readDataFromSqlResult(cacheMaxSize.getData(),
            CACHE_TYPE.getField(), CacheType.SHARD_REQUEST_CACHE.toString(), MetricsDB.MAX);
    if (!Double.isNaN(shardRequestCacheMaxSize)) {
      configResult.put(ResourceUtil.SHARD_REQUEST_CACHE_MAX_SIZE, shardRequestCacheMaxSize);
    }
    else {
      LOG.error("Shard Request cache max size is NaN");
    }
  }

  /**
   * collect config settings from the upstream metric flowunits and set them into the protobuf
   * message PerformanceControllerConfiguration. This will allow us to serialize / de-serialize
   * the config settings across grpc and send them to Decision Maker on elected master.
   * @return ResourceFlowUnit with HotNodeSummary. And HotNodeSummary carries PerformanceControllerConfiguration
   */
  @Override
  public NodeConfigFlowUnit operate() {
    counter += 1;
    for (MetricFlowUnit flowUnit : threadPool_queueCapacity.getFlowUnits()) {
      if (flowUnit.isEmpty()) {
        continue;
      }
      collectQueueCapacity(flowUnit);
    }
    for (MetricFlowUnit flowUnit : cacheMaxSize.getFlowUnits()) {
      if (flowUnit.isEmpty()) {
        continue;
      }
      collectCacheMaxSize(flowUnit);
    }
    if (counter == rcaPeriod) {
      counter = 0;
      NodeConfigFlowUnit flowUnits = new NodeConfigFlowUnit(System.currentTimeMillis(), new NodeKey(getInstanceDetails()));
      configResult.forEach(flowUnits::addConfig);
      return flowUnits;
    }
    else {
      return new NodeConfigFlowUnit(System.currentTimeMillis());
    }
  }
}
