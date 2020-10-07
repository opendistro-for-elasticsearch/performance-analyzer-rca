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

  private static final int DEFAULT_DAY_BREACH_THRESHOLD = 5;
  private static final int DEFAULT_WEEK_BREACH_THRESHOLD = 2;

  private BucketizedSlidingWindow dayMonitor;
  private BucketizedSlidingWindow weekMonitor;
  private int dayBreachThreshold;
  private int weekBreachThreshold;
  private boolean alarmHealthy = true;

  public JvmActionsAlarmMonitor(int dayBreachThreshold, int weekBreachThreshold) {
    dayMonitor = new BucketizedSlidingWindow((int) TimeUnit.DAYS.toMinutes(1), 30, TimeUnit.MINUTES);
    weekMonitor = new BucketizedSlidingWindow(4, 1, TimeUnit.DAYS);
    this.dayBreachThreshold = dayBreachThreshold;
    this.weekBreachThreshold = weekBreachThreshold;
  }

  public JvmActionsAlarmMonitor() {
    this(DEFAULT_DAY_BREACH_THRESHOLD, DEFAULT_WEEK_BREACH_THRESHOLD);
  }

  @Override
  public void recordIssue(long timeStamp, double value) {
    SlidingWindowData dataPoint = new SlidingWindowData(timeStamp, value);
    dayMonitor.next(dataPoint);
    // If we've breached the day threshold, record it as a bad day this week.
    if (dayMonitor.size() >= dayBreachThreshold) {
      weekMonitor.next(new SlidingWindowData(dataPoint.getTimeStamp(), dataPoint.getValue()));
    }
  }

  private void evaluateAlarm() {
    if (alarmHealthy) {
      if (weekMonitor.size() >= weekBreachThreshold) {
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

  public int getDayBreachThreshold() {
    return dayBreachThreshold;
  }

  public int getWeekBreachThreshold() {
    return weekBreachThreshold;
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
