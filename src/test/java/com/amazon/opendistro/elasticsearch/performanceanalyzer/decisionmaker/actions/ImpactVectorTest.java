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
