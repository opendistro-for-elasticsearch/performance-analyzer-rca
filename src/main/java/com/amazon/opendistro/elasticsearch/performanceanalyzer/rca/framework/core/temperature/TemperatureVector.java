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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.temperature;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import java.util.Comparator;
import javax.annotation.Nullable;

public class TemperatureVector {
    public static final String DIMENSION_KEY = "dimension";
    public static final String HEAT_VALUE_KEY = "heat_value";
    public static final String TOTAL_RAW_VALUE_KEY = "total_raw_value";

    public static class VectorValues {
        public static final int MIN = 0;
        public static final int MAX = 10;

        private final short HEATVALUE;
        private final double RAWVALUE;

        public VectorValues(short HEATVALUE, double RAWVALUE) {
            if (HEATVALUE < MIN || HEATVALUE > MAX) {
                String err = String.format("Only values between %d and %d allowed. Given: %d", MIN, MAX,
                        HEATVALUE);
                throw new IllegalArgumentException(err);
            }

            this.HEATVALUE = HEATVALUE;
            this.RAWVALUE = RAWVALUE;
        }

        /**
         * Units of a resource consumed is a number between 0 and 10 where 0 being cold(not using any
         * resource) and 10 being hot(almost all the units of the resource were consumed by it).
         * Temperature is calculated by determining what parts of 10 is consumed by the resource.
         */
        public static VectorValues calculate(double consumedByCandidate, double totalConsumption) {
            return new VectorValues((short) (consumedByCandidate * 10 / totalConsumption), totalConsumption);
        }

        public VectorValues diff(VectorValues b) {
            return new VectorValues((short) (HEATVALUE - b.HEATVALUE), RAWVALUE);
        }

        public boolean isGreaterThan(VectorValues b) {
            return HEATVALUE > b.HEATVALUE;
        }

        public Comparator<VectorValues> comparator() {
            return Comparator.comparingInt(o -> o.HEATVALUE);
        }

        public short getHeatValue() {
            return HEATVALUE;
        }

        public double getRawValue() {
            return RAWVALUE;
        }

        @Override
        public String toString() {
            return "" + HEATVALUE;
        }
    }

    /**
     * This array contains a normalized value per dimension.
     */
    private VectorValues[] vectorValues;

    public TemperatureVector() {
        this.vectorValues = new VectorValues[TemperatureDimension.values().length];
    }

    @Override
    public String toString() {
        return toJson().toString();
    }

    public JsonArray toJson() {
        JsonArray array = new JsonArray();
        for (TemperatureDimension dim : TemperatureDimension.values()) {
            VectorValues val = vectorValues[dim.ordinal()];
            if (val != null) {
                JsonObject jsonObject = new JsonObject();
                jsonObject.addProperty(DIMENSION_KEY, dim.NAME);
                jsonObject.addProperty(HEAT_VALUE_KEY, val.getHeatValue());
                jsonObject.addProperty(TOTAL_RAW_VALUE_KEY, val.getRawValue());
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
    public TemperatureVector.VectorValues getTemperatureVectorValue(TemperatureDimension dimension) {
        return vectorValues[dimension.ordinal()];
    }

    public void updateTemperatureForDimension(TemperatureDimension dimension,
                                              VectorValues vectorValue) {
        vectorValues[dimension.ordinal()] = new VectorValues(vectorValue.getHeatValue(), vectorValue.getRawValue());
    }
}
