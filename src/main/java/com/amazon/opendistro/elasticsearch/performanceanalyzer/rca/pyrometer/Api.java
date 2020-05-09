/*
 * Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 *  A copy of the License is located at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  or in the "license" file accompanying this file. This file is distributed
 *  on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *  express or implied. See the License for the specific language governing
 *  permissions and limitations under the License.
 */

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.pyrometer;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.temperature.CompactNodeSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.temperature.ShardProfileSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.temperature.HeatZoneAssigner;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.temperature.TemperatureVector;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;

/**
 * Although Pyrometer exists on all the nodes - data and master but the APIs are only accessible
 * from the master nodes. Pyrometer is able answer queries such as:
 * - All nodes by in Zone <i>Z</i> by dimension <i>D</i> order by <i>Temperature</i> in
 * descending order.
 * - The top <i>K</i> warm shards on the hottest nodes by dimension D over a period of h hours
 * -
 */
public class Api {
    enum Count {
        /**
         * This is to ask the API to return all the contenders.
         */
        ALL,

        /**
         * This is to return the top K contenders
         */
        TOP_K
    }

    enum SortOrder {
        ASCENDING,
        DESCENDING;
    }

    /**
     * Api class should be called only from elected master node.
     *
     * @return true if elected master or false otherwise.
     */
    private static boolean checkIfElectedMaster() {
        return true;
    }

    private static void failIfNotElectedMaster(String apiName) {
        if (!checkIfElectedMaster()) {
            StringBuilder builder = new StringBuilder();
            builder.append("Api (").append(apiName).append(") can only be called from the elected"
                    + " master node.");

            throw new IllegalStateException(builder.toString());
        }
    }

    /**
     * A cluster is considered imbalanced along a dimension if the temperature of the hottest
     * node and the coldest node are too far apart. How far is too far, is something to be
     * evaluated.
     *
     * @param dimension The dimension to consider.
     * @return The difference in temperature of the
     */
    public static boolean isClusterImbalanceAlongDimension(TemperatureVector.Dimension dimension) {
        failIfNotElectedMaster("getClusterImbalanceAlongDimension");
        // TODO: Calculate this
        return true;
    }


    /**
     * To get the list of all the nodes in the cluster order.
     *
     * @param dimension Temperature along this dimension will be considered to sort the nodes
     * @param zone      Only the nodes in this zone will be reported.
     * @param count     How many nodes to return in the result.
     * @param order     Will the nodes be sorted in the increasing order or decreasing order.
     * @return An ordered list of nodes.
     */
    public static @Nonnull
    List<CompactNodeSummary> getNodesForGivenZone(final TemperatureVector.Dimension dimension,
                                                  final HeatZoneAssigner.Zone zone,
                                                  final Count count,
                                                  final SortOrder order) {
        failIfNotElectedMaster("getNodes");
        return new ArrayList<>();
    }

    /**
     * @param dimension The temperature along this dimension will be used to sort the nodes.
     * @param count     How many nodes to return
     * @param order     Whether to sort the final nodes list in increasing or decreasing order.
     * @return A list of nodes ordered by the temperature Zone by the ordinal of    the enum.
     */
    public static @Nonnull
    Map<HeatZoneAssigner.Zone, List<CompactNodeSummary>> getNodesForAllZones(final TemperatureVector.Dimension dimension,
                                                                             final Count count,
                                                                             final SortOrder order) {
        failIfNotElectedMaster("getNodes");

        Map<HeatZoneAssigner.Zone, List<CompactNodeSummary>> nodes = new HashMap<>();

        for (HeatZoneAssigner.Zone zone : HeatZoneAssigner.Zone.values()) {
            nodes.put(zone, getNodesForGivenZone(dimension, zone, count, order));
        }

        return nodes;
    }

    /**
     * This API only provides shards for a particular node. Although this API is called on the
     * master Pyrometer instance, but it is asking for details of a particular node. The master
     * Pyrometer might need to fetch the details from the node in question to respond to this
     * request.
     *
     * @param node      This provides the nodeIp or nodeId. All other member values are invalid.
     * @param dimension The temperature for this dimension will be used to order the shards.
     * @param zone      Only the shards in this zone needs to be reported.
     * @param count     Whether we want all the shards or just the top K
     * @param sortOrder The sort order for the shards.
     * @return Returns all the shards
     */
    public static @Nonnull
    List<ShardProfileSummary> getShards(
            final CompactNodeSummary node,
            final TemperatureVector.Dimension dimension,
            final HeatZoneAssigner.Zone zone,
            final Count count,
            final SortOrder sortOrder) {
        List<ShardProfileSummary> shards = new ArrayList<>();


        return shards;
    }
}
