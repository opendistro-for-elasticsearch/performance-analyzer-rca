package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.integTests.framework.annotations;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.integTests.framework.configs.HostTag;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

/**
 * This specifies a table for a given metric. This annotation is a sub-field of the AMetric annotation.
 */
@Retention(RetentionPolicy.RUNTIME)
public @interface ATable {
  // Which host should emit this metric
  HostTag[] hostTag();

  // The data in tabular form.
  ATuple[] tuple();
}
