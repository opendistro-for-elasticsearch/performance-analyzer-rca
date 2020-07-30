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
import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.ModifyCacheCapacityAction;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.ResourceEnum;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Rca;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.flow_units.ResourceFlowUnit;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotClusterSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotResourceSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.cluster.BaseClusterRca;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.cluster.FieldDataCacheClusterRca;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.cluster.NodeKey;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.cluster.ShardRequestCacheClusterRca;
import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.List;

// TODO: 1. Read current cache capacity, total cache capacity, upper bound, lower bound from NodeConfigurationRca
public class CacheHealthDecider extends Decider {

    public static final String NAME = "cacheHealthDecider";

    private final FieldDataCacheClusterRca fieldDataCacheClusterRca;
    private final ShardRequestCacheClusterRca shardRequestCacheClusterRca;

    List<String> actionsByUserPriority = new ArrayList<>();
    private int counter = 0;

    public CacheHealthDecider(final long evalIntervalSeconds,
                              final int decisionFrequency,
                              final FieldDataCacheClusterRca fieldDataCacheClusterRca,
                              final ShardRequestCacheClusterRca shardRequestCacheClusterRca) {
        // TODO: Also consume NodeConfigurationRca
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

            final HotClusterSummary clusterSummary = flowUnit.getSummary();

            clusterSummary
                    .getHotNodeSummaryList()
                    .forEach(
                            hotNodeSummary -> {
                                final NodeKey esNode =
                                        new NodeKey(hotNodeSummary.getNodeID(), hotNodeSummary.getHostAddress());
                                for (final HotResourceSummary resource :
                                        hotNodeSummary.getHotResourceSummaryList()) {
                                    decision.addAction(
                                            computeBestAction(esNode, resource.getResource().getResourceEnum()));
                                }
                            });
        }
    }

    private void configureActionPriority() {
        // TODO: Input from user configured yml
        this.actionsByUserPriority.add(ModifyCacheCapacityAction.NAME);
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
                    getAction(actionName, esNode, cacheType, getNodeCacheCapacityInBytes(esNode, cacheType), true);
            if (action != null) {
                break;
            }
        }
        return action;
    }

    private Action getAction(final String actionName,
                             final NodeKey esNode,
                             final ResourceEnum cacheType,
                             final long currentCapacityInBytes,
                             final boolean increase) {
        if (ModifyCacheCapacityAction.NAME.equals(actionName)) {
            return configureCacheCapacity(esNode, cacheType, currentCapacityInBytes, increase);
        }
        return null;
    }

    private ModifyCacheCapacityAction configureCacheCapacity(
            final NodeKey esNode,
            final ResourceEnum cacheType,
            final long currentCapacityInBytes,
            final boolean increase) {
        final ModifyCacheCapacityAction action =
                new ModifyCacheCapacityAction(esNode, cacheType, currentCapacityInBytes, increase);
        if (action.isActionable()) {
            return action;
        }
        return null;
    }

    private long getNodeCacheCapacityInBytes(final NodeKey esNode, final ResourceEnum cacheType) {
        // TODO: use NodeConfigurationRca to return capacity, for now returning random value in Bytes
        if (cacheType.equals(ResourceEnum.FIELD_DATA_CACHE)) {
            return 1000L;
        }
        return 1000L;
    }
}
