package com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.deciders.jvm.old_gen;

public class Consts {
  public static class LEVEL_ONE_CONST {
    public static final double FIELD_DATA_CACHE_LOWER_BOUND = 0.1;
    public static final double SHARD_REQUEST_CACHE_LOWER_BOUND = 0.02;
    public static final int CACHE_ACTION_STEP_COUNT = 1;
  }
}
