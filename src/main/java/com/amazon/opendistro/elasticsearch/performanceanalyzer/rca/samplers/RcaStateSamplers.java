package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.samplers;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.stats.emitters.ISampler;

public class RcaStateSamplers {

  public static ISampler getRcaEnabledSampler() {
    return new RcaEnabledSampler();
  }
}
