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

import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.deciders.AlarmMonitor;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.aggregators.BucketizedSlidingWindow;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.aggregators.SlidingWindowData;
import com.google.common.annotations.VisibleForTesting;
import java.util.concurrent.TimeUnit;

public class JvmActionsAlarmMonitor implements AlarmMonitor {

  public static final int DAY_BREACH_THRESHOLD = 5;
  public static final int WEEK_BREACH_THRESHOLD = 2;

  private BucketizedSlidingWindow dayMonitor;
  private BucketizedSlidingWindow weekMonitor;
  private boolean alarmHealthy = true;

  public JvmActionsAlarmMonitor() {
    dayMonitor = new BucketizedSlidingWindow((int) TimeUnit.DAYS.toMinutes(1), 30, TimeUnit.MINUTES);
    weekMonitor = new BucketizedSlidingWindow(4, 1, TimeUnit.DAYS);
  }

  @Override
  public void recordIssue(long timeStamp, double value) {
    SlidingWindowData dataPoint = new SlidingWindowData(timeStamp, value);
    dayMonitor.next(dataPoint);
    // If we've breached the day threshold, record it as a bad day this week.
    if (dayMonitor.size() >= DAY_BREACH_THRESHOLD) {
      weekMonitor.next(new SlidingWindowData(dataPoint.getTimeStamp(), dataPoint.getValue()));
    }
  }

  private void evaluateAlarm() {
    if (alarmHealthy) {
      if (weekMonitor.size() >= WEEK_BREACH_THRESHOLD) {
        alarmHealthy = false;
      }
    } else {
      if (dayMonitor.size() == 0 && weekMonitor.size() == 0) {
        alarmHealthy = true;
      }
    }
  }

  @Override
  public boolean isHealthy() {
    evaluateAlarm();
    return alarmHealthy;
  }

  @VisibleForTesting
  BucketizedSlidingWindow getDayMonitor() {
    return dayMonitor;
  }

  @VisibleForTesting
  BucketizedSlidingWindow getWeekMonitor() {
    return weekMonitor;
  }

  @VisibleForTesting
  void setAlarmHealth(boolean isHealthy) {
    this.alarmHealthy = isHealthy;
  }
}
