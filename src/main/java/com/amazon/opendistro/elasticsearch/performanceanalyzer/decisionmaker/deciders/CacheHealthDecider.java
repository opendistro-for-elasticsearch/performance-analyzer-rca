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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.deciders;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.Action;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.ModifyCacheMaxSizeAction;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.ResourceEnum;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.flow_units.ResourceFlowUnit;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotClusterSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotNodeSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotResourceSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.ResourceUtil;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.cluster.BaseClusterRca;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.cluster.FieldDataCacheClusterRca;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.cluster.NodeKey;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.cluster.ShardRequestCacheClusterRca;
import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class CacheHealthDecider extends Decider {
    private static final Logger LOG = LogManager.getLogger(CacheHealthDecider.class);
    public static final String NAME = "cacheHealthDecider";

    private final FieldDataCacheClusterRca fieldDataCacheClusterRca;
    private final ShardRequestCacheClusterRca shardRequestCacheClusterRca;

    List<String> actionsByUserPriority = new ArrayList<>();
    private int counter = 0;

    public CacheHealthDecider(final long evalIntervalSeconds,
                              final int decisionFrequency,
                              final FieldDataCacheClusterRca fieldDataCacheClusterRca,
                              final ShardRequestCacheClusterRca shardRequestCacheClusterRca) {
        super(evalIntervalSeconds, decisionFrequency);

        this.fieldDataCacheClusterRca = fieldDataCacheClusterRca;
        this.shardRequestCacheClusterRca = shardRequestCacheClusterRca;

        configureActionPriority();
    }

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public Decision operate() {
        final ImmutableList<BaseClusterRca> cacheClusterRca =
            ImmutableList.<BaseClusterRca>builder()
                .add(shardRequestCacheClusterRca)
                .add(fieldDataCacheClusterRca)
                .build();

        Decision decision = new Decision(System.currentTimeMillis(), NAME);
        counter += 1;
        if (counter < decisionFrequency) {
            return decision;
        }
        counter = 0;

        // TODO: Tune only one resource at a time based on action priorities
        cacheClusterRca.forEach(rca -> getActionsFromRca(rca, decision));
        return decision;
    }

    private <R extends BaseClusterRca> void getActionsFromRca(
            final R cacheClusterRca, final Decision decision) {
        if (!cacheClusterRca.getFlowUnits().isEmpty()) {
            final ResourceFlowUnit<HotClusterSummary> flowUnit = cacheClusterRca.getFlowUnits().get(0);
            if (!flowUnit.hasResourceSummary()) {
                return;
            }

            final List<HotNodeSummary> clusterSummary = flowUnit.getSummary().getHotNodeSummaryList();

            for (final HotNodeSummary hotNodeSummary : clusterSummary) {
                final NodeKey esNode = new NodeKey(hotNodeSummary.getNodeID(), hotNodeSummary.getHostAddress());
                for (final HotResourceSummary resource : hotNodeSummary.getHotResourceSummaryList()) {
                    decision.addAction(computeBestAction(esNode, resource.getResource().getResourceEnum()));
                }
            }
        }
    }

    private void configureActionPriority() {
        // TODO: Input from user configured yml
        this.actionsByUserPriority.add(ModifyCacheMaxSizeAction.NAME);
    }

    /**
     * Evaluate the most relevant action for a node
     *
     * <p>Action relevance decided based on user configured priorities for now, this can be modified
     * to consume better signals going forward.
     */
    private Action computeBestAction(final NodeKey esNode, final ResourceEnum cacheType) {
        Action action = null;
        for (String actionName : actionsByUserPriority) {
            action =
                getAction(
                    actionName,
                    esNode,
                    cacheType,
                    getNodeCacheMaxSizeInBytes(esNode, cacheType),
                    getHeapMaxSizeInBytes(esNode),
                    true);
            if (action != null) {
                break;
            }
        }
        return action;
    }

    private Action getAction(final String actionName,
                             final NodeKey esNode,
                             final ResourceEnum cacheType,
                             final long currentMaxSizeInBytes,
                             final long heapMaxSizeInBytes,
                             final boolean increase) {
        if (currentMaxSizeInBytes == -1 || heapMaxSizeInBytes == -1) {
            return null;
        }
        if (ModifyCacheMaxSizeAction.NAME.equals(actionName)) {
            return configureCacheMaxSize(esNode, cacheType, currentMaxSizeInBytes, heapMaxSizeInBytes, increase);
        }
        return null;
    }

    private ModifyCacheMaxSizeAction configureCacheMaxSize(
            final NodeKey esNode,
            final ResourceEnum cacheType,
            final long currentMaxSizeInBytes,
            final long heapMaxSizeInBytes,
            final boolean increase) {
        final ModifyCacheMaxSizeAction action =
                new ModifyCacheMaxSizeAction(esNode, cacheType, currentMaxSizeInBytes, heapMaxSizeInBytes, increase);
        if (action.isActionable()) {
            return action;
        }
        return null;
    }

    private long getNodeCacheMaxSizeInBytes(final NodeKey esNode, final ResourceEnum cacheType) {
        try {
            if (cacheType.equals(ResourceEnum.FIELD_DATA_CACHE)) {
                return (long) getAppContext().getNodeConfigCache().get(esNode, ResourceUtil.FIELD_DATA_CACHE_MAX_SIZE);
            }
            return (long) getAppContext().getNodeConfigCache().get(esNode, ResourceUtil.SHARD_REQUEST_CACHE_MAX_SIZE);
        } catch (final Exception e) {
            LOG.error("Exception while reading cache max size from Node Config Cache", e);
        }
        // No action if value not present in the cache.
        // No action will be triggered as this value was wiped out from the cache
        return -1;
  }

  private long getHeapMaxSizeInBytes(final NodeKey esNode) {
      try {
          return (long) getAppContext().getNodeConfigCache().get(esNode, ResourceUtil.HEAP_MAX_SIZE);
      } catch (final Exception e) {
          LOG.error("Exception while reading heap max size from Node Config Cache", e);
      }
      // No action if value not present in the cache.
      // No action will be triggered as this value was wiped out from the cache
      return -1;
  }
}
