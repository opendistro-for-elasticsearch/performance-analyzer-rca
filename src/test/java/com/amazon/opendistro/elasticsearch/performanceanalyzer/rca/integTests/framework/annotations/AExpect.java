package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.integTests.framework.annotations;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.integTests.framework.configs.HostTag;
import java.lang.annotation.ElementType;
import java.lang.annotation.Repeatable;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * This annotation is used for validating the result. After invoking the test,
 * the RcaItRunner periodically checks the output and passes the test if it
 * passes the validator checks.
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@Repeatable(value = AExpect.Expectations.class)
public @interface AExpect {
  /**
   * @return What will be queried and matched against the expectations.
   */
  Type what();

  /**
   * Which node will be queried for this data ?
   *
   * @return The node to be queried for this
   */
  HostTag on();

  /**
   * How long shall we wait for the expected result to show up.
   *
   * @return timeout in seconds
   */
  long timeoutSeconds() default 60;

  /**
   * Which custom validator should be called with the current query-results.
   *
   * @return The class that will be used for validation.
   */
  Class validator();

  /**
   * Which RCA should we ask for from the SQLite or the rest API.
   *
   * @return The Rca class to look for.
   */
  Class forRca();

  /**
   * Currently supported places to look for RCA outputs are the SQLite file or by hitting the rest endpoint.
   */
  enum Type {
    REST_API,
    DB_QUERY
  }

  @Retention(RetentionPolicy.RUNTIME)
  @Target(ElementType.METHOD)
  @interface Expectations {
    AExpect[] value();
  }
}
