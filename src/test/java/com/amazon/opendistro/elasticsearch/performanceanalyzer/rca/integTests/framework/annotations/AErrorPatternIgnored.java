/*
 * Copyright 2021 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

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
   *
   * @return Patterns that will be considered before the error log. If multiple patterns are specified, then the presence of any of the
   *     them will ignore the log line.
   */
  String pattern();

  /**
   * Why should this error be ignored.
   *
   * @return The reason for ignoring the error.
   */
  String reason();

  @Retention(RetentionPolicy.RUNTIME)
  @Target(ElementType.METHOD)
  @interface Patterns {
    AErrorPatternIgnored[] value();
  }
}
