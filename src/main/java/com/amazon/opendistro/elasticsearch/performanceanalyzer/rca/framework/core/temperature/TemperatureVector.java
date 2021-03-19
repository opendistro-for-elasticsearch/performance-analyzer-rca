/*
 * Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.temperature;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import java.util.Comparator;
import javax.annotation.Nullable;

/**
 * This vector contains the normalized values across dimensions.
 * normalized[0] gives the normalized value of the CPU on a scale of 0-10.
 * This can be on a shard level as well as on a node level.
 * On a shard level the normalized value is computed taking the resource used
 * by that shard and the total used in the node.
 * On a node level the normalized value is computed by taking into account the
 * average usage by shards and the total used in the node.
 */

public class TemperatureVector {
    public static final String DIMENSION_KEY = "dimension";
    public static final String VALUE_KEY = "value";

    public static class NormalizedValue {
        public static final int MIN = 0;
        public static final int MAX = 10;

        private final short POINTS;

        public NormalizedValue(short heatValue) {
            if (heatValue < MIN || heatValue > MAX) {
                String err = String.format("Only values between %d and %d allowed. Given: %d", MIN, MAX,
                        heatValue);
                throw new IllegalArgumentException(err);
            }

            this.POINTS = heatValue;
        }

        /**
         * Units of a resource consumed is a number between 0 and 10 where 0 being cold(not using any
         * resource) and 10 being hot(almost all the units of the resource were consumed by it).
         * Temperature is calculated by determining what parts of 10 is consumed by the resource.
         */
        public static NormalizedValue calculate(double consumedByCandidate, double totalConsumption) {
            return new NormalizedValue((short) (consumedByCandidate * 10 / totalConsumption));
        }

        public NormalizedValue diff(NormalizedValue b) {
            return new NormalizedValue((short) (POINTS - b.POINTS));
        }

        public boolean isGreaterThan(NormalizedValue b) {
            return POINTS > b.POINTS;
        }

        public Comparator<NormalizedValue> comparator() {
            return Comparator.comparingInt(o -> o.POINTS);
        }

        public short getPOINTS() {
            return POINTS;
        }

        @Override
        public String toString() {
            return "" + POINTS;
        }
    }

    /**
     * This array contains a normalized value per dimension.
     */
    private NormalizedValue[] normalizedValues;

    public TemperatureVector() {
        normalizedValues = new NormalizedValue[TemperatureDimension.values().length];
    }

    @Override
    public String toString() {
        return toJson().toString();
    }

    public JsonArray toJson() {
        JsonArray array = new JsonArray();
        for (TemperatureDimension dim : TemperatureDimension.values()) {
            NormalizedValue val = normalizedValues[dim.ordinal()];
            if (val != null) {
                JsonObject jsonObject = new JsonObject();
                jsonObject.addProperty(DIMENSION_KEY, dim.NAME);
                jsonObject.addProperty(VALUE_KEY, val.toString());
                array.add(jsonObject);
            }
        }
        return array;
    }

    /**
     * This can be used to get temperature along a dimension.
     *
     * @param dimension one of the dime
     * @return The normalized temperature value along that dimension.
     */
    @Nullable
    public NormalizedValue getTemperatureFor(TemperatureDimension dimension) {
        return normalizedValues[dimension.ordinal()];
    }

    public void updateTemperatureForDimension(TemperatureDimension dimension,
                                              NormalizedValue normalizedValue) {
        normalizedValues[dimension.ordinal()] = normalizedValue;
    }
}
