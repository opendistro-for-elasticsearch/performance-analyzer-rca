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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class ImpactVector {

  public enum Dimension {
    HEAP,
    CPU,
    RAM,
    DISK,
    NETWORK
  }

  public enum Impact {
    NO_IMPACT,
    INCREASES_PRESSURE,
    DECREASES_PRESSURE
  }

  private Map<Dimension, Impact> impactMap = new HashMap<>();

  public ImpactVector() {
    for (Dimension d : Dimension.values()) {
      impactMap.put(d, Impact.NO_IMPACT);
    }
  }

  public Map<Dimension, Impact> getImpact() {
    return Collections.unmodifiableMap(impactMap);
  }

  public void increasesPressure(Dimension... dimensions) {
    for (Dimension dimension : dimensions) {
      impactMap.put(dimension, Impact.INCREASES_PRESSURE);
    }
  }

  public void decreasesPressure(Dimension... dimensions) {
    for (Dimension dimension : dimensions) {
      impactMap.put(dimension, Impact.DECREASES_PRESSURE);
    }
  }

  public void noImpact(Dimension... dimensions) {
    for (Dimension dimension : dimensions) {
      impactMap.put(dimension, Impact.NO_IMPACT);
    }
  }

  /**
   * Two ImpactVectors are equal if and only if they have the same impact for each of their
   * dimensions
   * @param o The other ImpactVector to compare with this
   * @return true if and only if this and o have the same impact for each of their dimensions
   */
  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ImpactVector that = (ImpactVector) o;
    return Objects.equals(impactMap, that.impactMap);
  }

  @Override
  public int hashCode() {
    return Objects.hash(impactMap);
  }
}
