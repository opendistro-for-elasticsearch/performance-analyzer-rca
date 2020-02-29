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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.temperature.profile.level;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.temperature.DetailedNodeTemperatureSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.temperature.TemperatureVector;
import java.util.Arrays;
import java.util.List;

public class FullNodeProfile {
    /**
     * A node has a temperature profile of its own. The temperature profile of a node is the mean
     * temperature along each dimension.
     */
    private TemperatureVector temperatureVector;

    /**
     * A node also has the complete list of shards in each dimension, broken down by the
     * different temperature zones.
     */
    private DetailedNodeTemperatureSummary[] nodeDimensionProfiles;

    private final String nodeId;
    private final String hostAddress;

    public FullNodeProfile(String nodeId, String hostAddress) {
        this.nodeId = nodeId;
        this.hostAddress = hostAddress;
        this.nodeDimensionProfiles =
                new DetailedNodeTemperatureSummary[TemperatureVector.Dimension.values().length];
        this.temperatureVector = new TemperatureVector();
    }

    public TemperatureVector getTemperatureVector() {
        return temperatureVector;
    }

    public List<DetailedNodeTemperatureSummary> getNodeDimensionProfiles() {
        return Arrays.asList(nodeDimensionProfiles);
    }

    public String getNodeId() {
        return nodeId;
    }

    public String getHostAddress() {
        return hostAddress;
    }

    public void updateNodeDimensionProfile(DetailedNodeTemperatureSummary nodeDimensionProfile) {
        TemperatureVector.Dimension dimension = nodeDimensionProfile.getProfileForDimension();
        this.nodeDimensionProfiles[dimension.ordinal()] = nodeDimensionProfile;
        temperatureVector.updateTemperatureForDimension(dimension, nodeDimensionProfile.getMeanTemperature());
    }

    public DetailedNodeTemperatureSummary getProfileByDimension(TemperatureVector.Dimension dimension) {
        return nodeDimensionProfiles[dimension.ordinal()];
    }
}
