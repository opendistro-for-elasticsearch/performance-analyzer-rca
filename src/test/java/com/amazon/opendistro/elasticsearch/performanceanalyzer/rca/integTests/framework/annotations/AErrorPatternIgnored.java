package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.integTests.framework.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Repeatable;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * This annotation can be used to ignore certain log messages for specific test methods.
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@Repeatable(AErrorPatternIgnored.Patterns.class)
public @interface AErrorPatternIgnored {
  /**
   * The substring, which if found in the error log line, we will ignore the error log.
   * @return Patterns that will be considered before the error log. If multiple patterns are specified, then the presence of any of the
   * them will ignore the log line.
   */
  String pattern();

  /**
   * Why should this error be ignored.
   * @return The reason for ignoring the error.
   */
  String reason();

  @Retention(RetentionPolicy.RUNTIME)
  @Target(ElementType.METHOD)
  @interface Patterns {
    AErrorPatternIgnored[] value();
  }
}
