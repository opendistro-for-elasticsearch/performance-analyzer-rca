/*
 * Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.deciders;

/**
 * AlarmMonitor evaluates and maintains the state of an alarm.
 *
 * <p>An alarm can either be healthy or unhealthy.
 */
public interface AlarmMonitor {

  /**
   * Invoked whenever an issue needs to be recorded with the monitor
   * @param timeStamp Issue timestamp in millis
   * @param value Issues can be recorded with an intensity value
   */
  public void recordIssue(long timeStamp, double value);

  public default void recordIssue() {
    recordIssue(System.currentTimeMillis(), 1);
  }

  /**
   * State of the alarm
   * @return true if alarm is in healthy state, false otherwise
   */
  public boolean isHealthy();

}
