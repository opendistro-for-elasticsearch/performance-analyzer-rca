package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.integTests.framework.annotations;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

/**
 * Should not be used on it own. This is a sub-field of the AMetric annotation. This represents one row of data
 * for a Metric. All the dimension values are specified as Strings.
 */
@Retention(RetentionPolicy.RUNTIME)
public @interface ATuple {
  String[] dimensionValues();

  double min();

  double max();

  double sum();

  double avg();
}
