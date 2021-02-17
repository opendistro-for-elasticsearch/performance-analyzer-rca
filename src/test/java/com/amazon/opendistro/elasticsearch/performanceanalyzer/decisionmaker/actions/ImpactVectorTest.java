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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions;

import static org.junit.Assert.assertEquals;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.ImpactVector.Dimension;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.ImpactVector.Impact;
import java.util.Map;
import org.junit.Test;

public class ImpactVectorTest {

  @Test
  public void testInit() {
    ImpactVector impactVector = new ImpactVector();
    Map<Dimension, Impact> impact = impactVector.getImpact();
    impact.forEach((k, v) -> assertEquals(impact.get(k), Impact.NO_IMPACT));
  }

  @Test
  public void testUpdateImpact() {
    ImpactVector impactVector = new ImpactVector();

    impactVector.increasesPressure(Dimension.HEAP, Dimension.CPU, Dimension.DISK);
    assertEquals(impactVector.getImpact().get(Dimension.HEAP), Impact.INCREASES_PRESSURE);
    assertEquals(impactVector.getImpact().get(Dimension.CPU), Impact.INCREASES_PRESSURE);
    assertEquals(impactVector.getImpact().get(Dimension.RAM), Impact.NO_IMPACT);
    assertEquals(impactVector.getImpact().get(Dimension.DISK), Impact.INCREASES_PRESSURE);
    assertEquals(impactVector.getImpact().get(Dimension.NETWORK), Impact.NO_IMPACT);

    impactVector.decreasesPressure(Dimension.RAM, Dimension.NETWORK);
    assertEquals(impactVector.getImpact().get(Dimension.HEAP), Impact.INCREASES_PRESSURE);
    assertEquals(impactVector.getImpact().get(Dimension.CPU), Impact.INCREASES_PRESSURE);
    assertEquals(impactVector.getImpact().get(Dimension.RAM), Impact.DECREASES_PRESSURE);
    assertEquals(impactVector.getImpact().get(Dimension.DISK), Impact.INCREASES_PRESSURE);
    assertEquals(impactVector.getImpact().get(Dimension.NETWORK), Impact.DECREASES_PRESSURE);

    impactVector.noImpact(Dimension.DISK, Dimension.RAM);
    assertEquals(impactVector.getImpact().get(Dimension.HEAP), Impact.INCREASES_PRESSURE);
    assertEquals(impactVector.getImpact().get(Dimension.CPU), Impact.INCREASES_PRESSURE);
    assertEquals(impactVector.getImpact().get(Dimension.RAM), Impact.NO_IMPACT);
    assertEquals(impactVector.getImpact().get(Dimension.DISK), Impact.NO_IMPACT);
    assertEquals(impactVector.getImpact().get(Dimension.NETWORK), Impact.DECREASES_PRESSURE);
  }
}
