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

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.temperature.TemperatureVector;
import com.google.gson.JsonObject;
import javax.annotation.Nullable;

public class ShardProfile {
    private final String indexName;
    private final int shardId;

    private final TemperatureVector temperatureVector;


    public ShardProfile(String indexName, int shardId) {
        this.indexName = indexName;
        this.shardId = shardId;
        this.temperatureVector = new TemperatureVector();
    }

    @Override
    public String toString() {
        return "Shard{"
                + "indexName='" + indexName
                + ", shardId=" + shardId
                + ", temp=" + temperatureVector
                + '}';
    }

    public JsonObject toJson() {
        JsonObject summaryObj = new JsonObject();
        summaryObj.addProperty("index_name", indexName);
        summaryObj.addProperty("shard_id", shardId);
        summaryObj.add("temperature", temperatureVector.toJson());
        return summaryObj;
    }

    @Nullable
    public TemperatureVector.NormalizedValue getHeatInDimension(TemperatureVector.Dimension dimension) {
        return temperatureVector.getTemperatureFor(dimension);
    }

    public void addTemperatureForDimension(TemperatureVector.Dimension dimension,
                                           TemperatureVector.NormalizedValue value) {
        TemperatureVector.NormalizedValue oldValue = getHeatInDimension(dimension);
        if (oldValue != null) {
            String err = String.format("Trying to update the temperature along dimension '%s' of "
                    + "shard '%s' twice. OldValue: '%s', newValue: '%s'", dimension, toString(),
                    oldValue, value);
            throw new IllegalArgumentException(err);
        }
        temperatureVector.updateTemperatureForDimension(dimension, value);
    }
}
