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

import static com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.ResourceUtil.FIELD_DATA_CACHE_EVICTION;
import static com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.cache.CacheUtil.getCacheMaxSize;
import static com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.cache.CacheUtil.isSizeThresholdExceeded;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.FlowUnitMessage;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.Resource;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metricsdb.MetricsDB;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.configs.CacheConfig;
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

/**
 * Field Data Cache RCA is to identify when the cache is unhealthy(thrashing) and otherwise, healthy.
 * The dimension we are using for this analysis is cache eviction count, cache current weight(size) and
 * cache max weight(size) configured.
 * Note : For Field Data Cache, Hit and Miss metrics aren't available.
 *
 * <p>Cache eviction within Elasticsearch happens in following scenarios :
 * <ol>
 *   <li>Mutation to Cache (Entry Insertion/Promotion and Manual Invalidation)
 *   <li>Explicit call to refresh()
 * </ol>
 *
 * <p>Cache Eviction requires either cache weight exceeds maximum weight OR the entry TTL is expired.
 * For Field Data Cache, no expire setting is present, so only in case of cache_weight exceeding the
 * max_cache_weight, eviction(removal from Cache Map and LRU linked List, entry updated to EVICTED)
 * happens.
 *
 * <p>Contrarily, the Cache Invalidation is performed manually on cache clear() and index close()
 * invocation, with removalReason as INVALIDATED and a force eviction is performed to ensure cleanup.
 *
 * <p>This RCA reads 'fieldDataCacheEvictions', 'fieldDataCacheSizeGroupByOperation' and
 * 'fieldDataCacheMaxSizeGroupByOperation' from upstream metrics and maintains a collector
 * which keeps track of the time window period(tp) where we repeatedly see evictions for the last
 * tp duration. This RCA is marked as unhealthy if tp is above the threshold(300 seconds) and
 * cache size exceeds the max cache size configured.
 *
 */
public class FieldDataCacheRca extends Rca<ResourceFlowUnit<HotNodeSummary>> {
    private static final Logger LOG = LogManager.getLogger(FieldDataCacheRca.class);
    private static final long EVICTION_THRESHOLD_TIME_PERIOD_IN_MILLISECOND = TimeUnit.SECONDS.toMillis(300);

    private final Metric fieldDataCacheEvictions;
    private final Metric fieldDataCacheSizeGroupByOperation;

    private final int rcaPeriod;
    private int counter;
    private double cacheSizeThreshold;
    protected Clock clock;
    private final CacheEvictionCollector cacheEvictionCollector;


    public <M extends Metric> FieldDataCacheRca(final int rcaPeriod,
                                                final M fieldDataCacheEvictions,
                                                final M fieldDataCacheSizeGroupByOperation) {
        super(5);
        this.rcaPeriod = rcaPeriod;
        this.fieldDataCacheEvictions = fieldDataCacheEvictions;
        this.fieldDataCacheSizeGroupByOperation = fieldDataCacheSizeGroupByOperation;
        this.counter = 0;
        this.cacheSizeThreshold = CacheConfig.DEFAULT_FIELD_DATA_CACHE_SIZE_THRESHOLD;
        this.clock = Clock.systemUTC();
        this.cacheEvictionCollector = new CacheEvictionCollector(FIELD_DATA_CACHE_EVICTION,
                fieldDataCacheEvictions, EVICTION_THRESHOLD_TIME_PERIOD_IN_MILLISECOND);
    }

    @VisibleForTesting
    public void setClock(Clock clock) {
        this.clock = clock;
    }

    @Override
    public ResourceFlowUnit<HotNodeSummary> operate() {
        counter += 1;
        long currTimestamp = clock.millis();

        cacheEvictionCollector.collect(currTimestamp);
        if (counter >= rcaPeriod) {
            ResourceContext context;
            HotNodeSummary nodeSummary;

            InstanceDetails instanceDetails = getInstanceDetails();
            double fieldDataCacheMaxSizeInBytes = getCacheMaxSize(
                    getAppContext(), new NodeKey(instanceDetails), ResourceUtil.FIELD_DATA_CACHE_MAX_SIZE);
            LOG.info("MOCHI, fieldDataCacheMaxSizeInBytes: {}", fieldDataCacheMaxSizeInBytes);
            Boolean exceedsSizeThreshold = isSizeThresholdExceeded(
                    fieldDataCacheSizeGroupByOperation, fieldDataCacheMaxSizeInBytes, cacheSizeThreshold);
            if (cacheEvictionCollector.isUnhealthy(currTimestamp) && exceedsSizeThreshold) {
                context = new ResourceContext(Resources.State.UNHEALTHY);
                nodeSummary = new HotNodeSummary(instanceDetails.getInstanceId(), instanceDetails.getInstanceIp());
                nodeSummary.appendNestedSummary(cacheEvictionCollector.generateSummary(currTimestamp));
            }
            else {
                context = new ResourceContext(Resources.State.HEALTHY);
                nodeSummary = null;
            }

            counter = 0;
            return new ResourceFlowUnit<>(currTimestamp, context, nodeSummary, !instanceDetails.getIsMaster());
        }
        else {
            return new ResourceFlowUnit<>(currTimestamp);
        }
    }

    /**
     * read threshold values from rca.conf
     * @param conf RcaConf object
     */
    @Override
    public void readRcaConf(RcaConf conf) {
        CacheConfig configObj = conf.getCacheConfig();
        cacheSizeThreshold = configObj.getFieldDataCacheSizeThreshold();
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
     * A collector class to collect eviction metrics
     */
    private static class CacheEvictionCollector {
        private final Resource cache;
        private final Metric cacheEvictionMetrics;
        private boolean hasEvictions;
        private long evictionTimestamp;
        private long evictionTimePeriodThreshold;

        private CacheEvictionCollector(final Resource cache, final Metric cacheEvictionMetrics,
                                       final long threshold) {
            this.cache = cache;
            this.cacheEvictionMetrics = cacheEvictionMetrics;
            this.hasEvictions = false;
            this.evictionTimestamp = 0;
            this.evictionTimePeriodThreshold = threshold;
        }

        public void collect(final long currTimestamp) {
            for (MetricFlowUnit flowUnit : cacheEvictionMetrics.getFlowUnits()) {
                if (flowUnit.isEmpty() || flowUnit.getData() == null) {
                    continue;
                }

                double evictionCount = flowUnit.getData().stream().mapToDouble(
                        record -> record.getValue(MetricsDB.MAX, Double.class)).sum();
                if (!Double.isNaN(evictionCount)) {
                    if (evictionCount > 0) {
                        if (!hasEvictions) {
                            evictionTimestamp = currTimestamp;
                        }
                        hasEvictions = true;
                    }
                    else {
                        hasEvictions = false;
                    }
                }
                else {
                    LOG.error("Failed to parse metric from cache {}", cache.toString());
                }
            }
        }

        public boolean isUnhealthy(final long currTimestamp) {
            return hasEvictions && (currTimestamp - evictionTimestamp) >= evictionTimePeriodThreshold;
        }

        private HotResourceSummary generateSummary(final long currTimestamp) {
            HotResourceSummary resourceSummary = null;
            if (isUnhealthy(currTimestamp)) {
                resourceSummary = new HotResourceSummary(cache,
                        TimeUnit.MILLISECONDS.toSeconds(evictionTimePeriodThreshold),
                        TimeUnit.MILLISECONDS.toSeconds(currTimestamp - evictionTimestamp),
                        0);
            }
            return resourceSummary;
        }
    }
}
