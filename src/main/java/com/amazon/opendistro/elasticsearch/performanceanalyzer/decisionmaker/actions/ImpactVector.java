package com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions;

import java.util.HashMap;
import java.util.Map;

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
    return impactMap;
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
}
