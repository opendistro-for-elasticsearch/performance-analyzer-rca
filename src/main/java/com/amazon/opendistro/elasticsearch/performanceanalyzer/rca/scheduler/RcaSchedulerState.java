package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.scheduler;

public enum RcaSchedulerState {
  STATE_NOT_STARTED,
  STATE_STARTED,
  STATE_STOPPED,
  STATE_STOPPED_DUE_TO_EXCEPTION
}
