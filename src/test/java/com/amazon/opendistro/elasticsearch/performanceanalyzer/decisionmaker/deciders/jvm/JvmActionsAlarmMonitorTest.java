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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.deciders.jvm;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.deciders.AlarmMonitor;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.aggregators.SlidingWindowData;
import java.util.concurrent.TimeUnit;
import org.junit.Test;

public class JvmActionsAlarmMonitorTest {

  @Test
  public void testInit() {
    AlarmMonitor monitor = new JvmActionsAlarmMonitor();
    assertTrue(monitor.isHealthy());
  }

  @Test
  public void testFlipToUnhealthy() {
    AlarmMonitor monitor = new JvmActionsAlarmMonitor();
    JvmActionsAlarmMonitor jvmMonitor = (JvmActionsAlarmMonitor) monitor;
    long startTimeInMins = TimeUnit.MILLISECONDS.toMinutes(System.currentTimeMillis());

    // Record issues and breach day threshold
    // (31 issues per bucket as we include boundary data points)
    long currTime;
    long thresholdBreachTS = startTimeInMins + 31 * jvmMonitor.DAY_BREACH_THRESHOLD;
    for (currTime = startTimeInMins; currTime < thresholdBreachTS; currTime++) {
      monitor.recordIssue(TimeUnit.MINUTES.toMillis(currTime), 1);
    }
    assertEquals(5, jvmMonitor.getDayMonitor().size());
    assertEquals(1, jvmMonitor.getWeekMonitor().size());
    assertTrue(monitor.isHealthy());

    // More issues within the same day do not add to week monitor
    for (currTime = thresholdBreachTS; currTime < thresholdBreachTS + 120; currTime++) {
      monitor.recordIssue(TimeUnit.MINUTES.toMillis(currTime), 1);
    }
    assertEquals(1, jvmMonitor.getWeekMonitor().size());
    assertTrue(jvmMonitor.getDayMonitor().size() > 5);
    assertTrue(monitor.isHealthy());

    // Add issues after 2 days
    currTime += TimeUnit.DAYS.toMinutes(2);
    for (int i = 0; i < jvmMonitor.DAY_BREACH_THRESHOLD; i++) {
      currTime += 31;
      monitor.recordIssue(TimeUnit.MINUTES.toMillis(currTime), 1);
    }
    assertEquals(jvmMonitor.DAY_BREACH_THRESHOLD, jvmMonitor.getDayMonitor().size());
    assertEquals(2, jvmMonitor.getWeekMonitor().size());
    assertFalse(monitor.isHealthy());
  }

  @Test
  public void testFlipToHealthy() {
    AlarmMonitor monitor = new JvmActionsAlarmMonitor();
    JvmActionsAlarmMonitor jvmMonitor = (JvmActionsAlarmMonitor) monitor;
    jvmMonitor.setAlarmHealth(false);

    // Since both monitors are empty, the alarm evaluates to healthy
    assertTrue(monitor.isHealthy());

    // Only day issues present
    jvmMonitor.setAlarmHealth(false);
    long currTime = TimeUnit.MILLISECONDS.toMinutes(System.currentTimeMillis());
    monitor.recordIssue(TimeUnit.MINUTES.toMillis(currTime), 1);
    assertEquals(1, jvmMonitor.getDayMonitor().size());
    assertEquals(0, jvmMonitor.getWeekMonitor().size());
    assertFalse(monitor.isHealthy());
  }

  @Test
  public void testFlipToHealthyWithWeekMonitorFlagged() {
    AlarmMonitor monitor = new JvmActionsAlarmMonitor();
    JvmActionsAlarmMonitor jvmMonitor = (JvmActionsAlarmMonitor) monitor;
    jvmMonitor.setAlarmHealth(false);

    jvmMonitor.getWeekMonitor().next(new SlidingWindowData(System.currentTimeMillis(), 1));
    assertEquals(0, jvmMonitor.getDayMonitor().size());
    assertEquals(1, jvmMonitor.getWeekMonitor().size());
    assertFalse(monitor.isHealthy());
  }
}
