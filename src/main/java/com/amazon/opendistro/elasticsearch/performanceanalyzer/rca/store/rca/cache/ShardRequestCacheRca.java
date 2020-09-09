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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.cache;

import static com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.ResourceUtil.SHARD_REQUEST_CACHE_EVICTION;
import static com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.ResourceUtil.SHARD_REQUEST_CACHE_HIT;
import static com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.cache.CacheUtil.getCacheMaxSize;
import static com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.cache.CacheUtil.isSizeThresholdExceeded;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.FlowUnitMessage;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.Resource;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metricsdb.MetricsDB;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.configs.FieldDataCacheRcaConfig;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.configs.ShardRequestCacheRcaConfig;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Metric;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Rca;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Resources;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.contexts.ResourceContext;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.flow_units.MetricFlowUnit;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.flow_units.ResourceFlowUnit;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotNodeSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotResourceSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.ResourceUtil;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.RcaConf;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.util.InstanceDetails;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.scheduler.FlowUnitOperationArgWrapper;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.cluster.NodeKey;
import com.google.common.annotations.VisibleForTesting;
import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.Record;
import org.jooq.Result;

/**
 * Shard Request Cache RCA is to identify when the cache is unhealthy(thrashing) and otherwise, healthy.
 * The dimension we are using for this analysis is cache eviction, hit count, cache current weight(size)
 * and cache max weight(size) configured.
 *
 * <p>Cache eviction within Elasticsearch happens in following scenarios:
 * <ol>
 *   <li> Mutation to Cache (Entry Insertion/Promotion and Manual Invalidation)
 *   <li> Explicit call to refresh()
 * </ol>
 *
 * <p>Cache Eviction requires either cache weight exceeds maximum weight OR the entry TTL is expired.
 * For Shard Request Cache, TTL is defined via `indices.requests.cache.expire` setting which is never used
 * in production clusters and only provided for backward compatibility, thus we ignore time based evictions.
 * The weight based evictions(removal from Cache Map and LRU linked List with entry updated to EVICTED) occur
 * when the cache_weight exceeds the max_cache_weight, eviction.
 *
 * <p>The Entry Invalidation is performed manually on cache clear(), index close() and for cached results from
 * timed-out requests. A scheduled runnable, running every 10 minutes cleans up all the invalidated entries which
 * have not been read/written to since invalidation.
 *
 * <p>The Cache Hit and Eviction metric presence implies cache is undergoing frequent load and eviction or undergoing
 * scheduled cleanup for entries which had timed-out during execution.
 *
 * <p>This RCA reads 'shardRequestCacheEvictions', 'shardRequestCacheHits', 'shardRequestCacheSizeGroupByOperation' and
 * 'shardRequestCacheMaxSizeInBytes' from upstream metrics and maintains collectors which keep track of time
 * window period(tp) where we repeatedly see evictions and hits for the last tp duration. This RCA is marked as unhealthy
 * if tp we find tp is above the threshold(300 seconds) and cache size exceeds the max cache size configured.
 *
 */
public class ShardRequestCacheRca extends Rca<ResourceFlowUnit<HotNodeSummary>> {
    private static final Logger LOG = LogManager.getLogger(ShardRequestCacheRca.class);

    private final Metric shardRequestCacheEvictions;
    private final Metric shardRequestCacheHits;
    private final Metric shardRequestCacheSizeGroupByOperation;
    private final int rcaPeriod;
    private int counter;
    private double cacheSizeThreshold;
    protected Clock clock;
    private final CacheCollector cacheEvictionCollector;
    private final CacheCollector cacheHitCollector;

    public <M extends Metric> ShardRequestCacheRca(final int rcaPeriod,
                                                   final M shardRequestCacheEvictions,
                                                   final M shardRequestCacheHits,
                                                   final M shardRequestCacheSizeGroupByOperation) {
        super(5);
        this.rcaPeriod = rcaPeriod;
        this.shardRequestCacheEvictions = shardRequestCacheEvictions;
        this.shardRequestCacheHits = shardRequestCacheHits;
        this.shardRequestCacheSizeGroupByOperation = shardRequestCacheSizeGroupByOperation;
        this.counter = 0;
        this.cacheSizeThreshold = ShardRequestCacheRcaConfig.DEFAULT_SHARD_REQUEST_CACHE_SIZE_THRESHOLD;
        this.clock = Clock.systemUTC();
        this.cacheEvictionCollector =
                new CacheCollector(
                        SHARD_REQUEST_CACHE_EVICTION,
                        shardRequestCacheEvictions,
                        ShardRequestCacheRcaConfig.DEFAULT_SHARD_REQUEST_COLLECTOR_TIME_PERIOD_IN_SEC);
        this.cacheHitCollector =
                new CacheCollector(
                        SHARD_REQUEST_CACHE_HIT,
                        shardRequestCacheHits,
                        ShardRequestCacheRcaConfig.DEFAULT_SHARD_REQUEST_COLLECTOR_TIME_PERIOD_IN_SEC);
    }

    @VisibleForTesting
    public void setClock(Clock clock) {
        this.clock = clock;
    }

    @Override
    public ResourceFlowUnit operate() {
        counter += 1;
        long currTimestamp = clock.millis();

        cacheEvictionCollector.collect(currTimestamp);
        cacheHitCollector.collect(currTimestamp);
        if (counter >= rcaPeriod) {
            ResourceContext context;
            InstanceDetails instanceDetails = getInstanceDetails();
            HotNodeSummary nodeSummary = new HotNodeSummary(instanceDetails.getInstanceId(), instanceDetails.getInstanceIp());

            double shardRequestCacheMaxSizeInBytes = getCacheMaxSize(
                    getAppContext(), new NodeKey(instanceDetails), ResourceUtil.SHARD_REQUEST_CACHE_MAX_SIZE);
            Boolean exceedsSizeThreshold = isSizeThresholdExceeded(
                    shardRequestCacheSizeGroupByOperation, shardRequestCacheMaxSizeInBytes, cacheSizeThreshold);

            // if eviction and hit counts persists in last 5 minutes and cache size exceeds max cache size * threshold percentage,
            // the cache is considered as unhealthy
            if (cacheEvictionCollector.isUnhealthy(currTimestamp)
                    && cacheHitCollector.isUnhealthy(currTimestamp)
                    && exceedsSizeThreshold) {
                context = new ResourceContext(Resources.State.UNHEALTHY);
                nodeSummary.appendNestedSummary(cacheEvictionCollector.generateSummary(currTimestamp));
            } else {
                context = new ResourceContext(Resources.State.HEALTHY);
            }

            counter = 0;
            return new ResourceFlowUnit<>(currTimestamp, context, nodeSummary, !instanceDetails.getIsMaster());
        } else {
            return new ResourceFlowUnit<>(currTimestamp);
        }
    }

    /**
     * read threshold values from rca.conf
     *
     * @param conf RcaConf object
     */
    @Override
    public void readRcaConf(RcaConf conf) {
        ShardRequestCacheRcaConfig configObj = conf.getShardRequestCacheRcaConfig();
        cacheSizeThreshold = configObj.getShardRequestCacheSizeThreshold();
        long cacheCollectorTimePeriodInSec =
                TimeUnit.SECONDS.toMillis(configObj.getShardRequestCollectorTimePeriodInSec());
        cacheHitCollector.setCollectorTimePeriod(cacheCollectorTimePeriodInSec);
        cacheEvictionCollector.setCollectorTimePeriod(cacheCollectorTimePeriodInSec);
    }

    @Override
    public void generateFlowUnitListFromWire(FlowUnitOperationArgWrapper args) {
        final List<FlowUnitMessage> flowUnitMessages =
                args.getWireHopper().readFromWire(args.getNode());
        List<ResourceFlowUnit<HotNodeSummary>> flowUnitList = new ArrayList<>();
        LOG.debug("rca: Executing fromWire: {}", this.getClass().getSimpleName());
        for (FlowUnitMessage flowUnitMessage : flowUnitMessages) {
            flowUnitList.add(ResourceFlowUnit.buildFlowUnitFromWrapper(flowUnitMessage));
        }
        setFlowUnits(flowUnitList);
    }

    /**
     * A collector class to collect metrics (eviction and hit) for cache
     */
    private static class CacheCollector {
        private final Resource cache;
        private final Metric cacheMetrics;
        private boolean hasMetric;
        private long metricTimestamp;
        private long metricTimePeriodInMillis;

        public CacheCollector(final Resource cache, final Metric cacheMetrics, final int metricTimePeriodInSec) {
            this.cache = cache;
            this.cacheMetrics = cacheMetrics;
            this.hasMetric = false;
            this.metricTimestamp = 0;
            this.metricTimePeriodInMillis = TimeUnit.SECONDS.toMillis(metricTimePeriodInSec);
        }

        public void setCollectorTimePeriod(long metricTimePeriodInMillis) {
            this.metricTimePeriodInMillis = metricTimePeriodInMillis;
        }

        public void collect(final long currTimestamp) {
            for (MetricFlowUnit flowUnit : cacheMetrics.getFlowUnits()) {
                if (flowUnit.isEmpty()) {
                    continue;
                }

                Result<Record> records = flowUnit.getData();
                double metricCount = records.stream().mapToDouble(
                        record -> record.getValue(MetricsDB.MAX, Double.class)).sum();
                if (!Double.isNaN(metricCount)) {
                    if (metricCount > 0) {
                        if (!hasMetric) {
                            metricTimestamp = currTimestamp;
                        }
                        hasMetric = true;
                    } else {
                        hasMetric = false;
                    }
                } else {
                    LOG.error("Failed to parse metric from cache {}", cache.toString());
                }
            }
        }

        public boolean isUnhealthy(final long currTimestamp) {
            return hasMetric && (currTimestamp - metricTimestamp) >= metricTimePeriodInMillis;
        }

        private HotResourceSummary generateSummary(final long currTimestamp) {
            return new HotResourceSummary(cache,
                    TimeUnit.MILLISECONDS.toSeconds(metricTimePeriodInMillis),
                    TimeUnit.MILLISECONDS.toSeconds(currTimestamp - metricTimestamp),
                    0);
        }
    }
}
