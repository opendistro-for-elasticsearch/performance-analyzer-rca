package com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions;

import static org.junit.Assert.assertEquals;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.ImpactVector.Impact;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.ImpactVector.Resource;
import java.util.Map;
import org.junit.Test;

public class ImpactVectorTest {

  @Test
  public void testInit() {
    ImpactVector impactVector = new ImpactVector();
    Map<Resource, Impact> impact = impactVector.getImpact();
    impact.forEach((k, v) -> assertEquals(impact.get(k), Impact.NO_IMPACT));
  }

  @Test
  public void testUpdateImpact() {
    ImpactVector impactVector = new ImpactVector();

    impactVector.increasesPressure(Resource.HEAP, Resource.CPU, Resource.DISK);
    assertEquals(impactVector.getImpact().get(Resource.HEAP), Impact.INCREASES_PRESSURE);
    assertEquals(impactVector.getImpact().get(Resource.CPU), Impact.INCREASES_PRESSURE);
    assertEquals(impactVector.getImpact().get(Resource.RAM), Impact.NO_IMPACT);
    assertEquals(impactVector.getImpact().get(Resource.DISK), Impact.INCREASES_PRESSURE);
    assertEquals(impactVector.getImpact().get(Resource.NETWORK), Impact.NO_IMPACT);

    impactVector.decreasesPressure(Resource.RAM, Resource.NETWORK);
    assertEquals(impactVector.getImpact().get(Resource.HEAP), Impact.INCREASES_PRESSURE);
    assertEquals(impactVector.getImpact().get(Resource.CPU), Impact.INCREASES_PRESSURE);
    assertEquals(impactVector.getImpact().get(Resource.RAM), Impact.DECREASES_PRESSURE);
    assertEquals(impactVector.getImpact().get(Resource.DISK), Impact.INCREASES_PRESSURE);
    assertEquals(impactVector.getImpact().get(Resource.NETWORK), Impact.DECREASES_PRESSURE);

    impactVector.noImpact(Resource.DISK, Resource.RAM);
    assertEquals(impactVector.getImpact().get(Resource.HEAP), Impact.INCREASES_PRESSURE);
    assertEquals(impactVector.getImpact().get(Resource.CPU), Impact.INCREASES_PRESSURE);
    assertEquals(impactVector.getImpact().get(Resource.RAM), Impact.NO_IMPACT);
    assertEquals(impactVector.getImpact().get(Resource.DISK), Impact.NO_IMPACT);
    assertEquals(impactVector.getImpact().get(Resource.NETWORK), Impact.DECREASES_PRESSURE);
  }
}
