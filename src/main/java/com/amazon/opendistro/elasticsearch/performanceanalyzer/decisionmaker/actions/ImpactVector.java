package com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions;

import java.util.HashMap;
import java.util.Map;

public class ImpactVector {

  public enum Resource {
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

  private Map<Resource, Impact> impactMap = new HashMap<>();

  public ImpactVector() {
    for (Resource r : Resource.values()) {
      impactMap.put(r, Impact.NO_IMPACT);
    }
  }

  public Map<Resource, Impact> getImpact() {
    return impactMap;
  }

  public void increasesPressure(Resource... resources) {
    for (Resource resource : resources) {
      impactMap.put(resource, Impact.INCREASES_PRESSURE);
    }
  }

  public void decreasesPressure(Resource... resources) {
    for (Resource resource : resources) {
      impactMap.put(resource, Impact.DECREASES_PRESSURE);
    }
  }

  public void noImpact(Resource... resources) {
    for (Resource resource : resources) {
      impactMap.put(resource, Impact.NO_IMPACT);
    }
  }
}
