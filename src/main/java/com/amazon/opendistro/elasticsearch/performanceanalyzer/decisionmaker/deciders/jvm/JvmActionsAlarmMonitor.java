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
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

public class JvmActionsAlarmMonitor implements AlarmMonitor {

  private static final int DEFAULT_DAY_BREACH_THRESHOLD = 5;
  private static final int DEFAULT_WEEK_BREACH_THRESHOLD = 2;
  private static final String DAY_PREFIX = "day-";
  private static final String WEEK_PREFIX = "week-";

  public static final int DAY_MONITOR_BUCKET_WINDOW_MINUTES = 30;
  public static final int WEEK_MONITOR_BUCKET_WINDOW_MINUTES = 86400;

  private BucketizedSlidingWindow dayMonitor;
  private BucketizedSlidingWindow weekMonitor;
  private int dayBreachThreshold;
  private int weekBreachThreshold;
  private boolean alarmHealthy = true;

  public JvmActionsAlarmMonitor(int dayBreachThreshold, int weekBreachThreshold, @Nullable Path persistencePath) {
    Path dayMonitorPath = null;
    Path weekMonitorPath = null;
    if (persistencePath != null) {
      Path persistenceBase = persistencePath.getParent();
      Path persistenceFile = persistencePath.getFileName();
      if (persistenceBase != null && persistenceFile != null) {
        dayMonitorPath = Paths.get(persistenceBase.toString(), DAY_PREFIX + persistenceFile.toString());
        weekMonitorPath = Paths.get(persistenceBase.toString(), WEEK_PREFIX + persistenceFile.toString());
      }
    }
    dayMonitor = new BucketizedSlidingWindow((int) TimeUnit.DAYS.toMinutes(1), 30, TimeUnit.MINUTES, dayMonitorPath);
    weekMonitor = new BucketizedSlidingWindow(4, 1, TimeUnit.DAYS, weekMonitorPath);
    this.dayBreachThreshold = dayBreachThreshold;
    this.weekBreachThreshold = weekBreachThreshold;
  }

  public JvmActionsAlarmMonitor(int dayBreachThreshold, int weekBreachThreshold) {
    this(dayBreachThreshold, weekBreachThreshold, null);
  }

  public JvmActionsAlarmMonitor(@Nullable Path persistencePath) {
    this(DEFAULT_DAY_BREACH_THRESHOLD, DEFAULT_WEEK_BREACH_THRESHOLD, persistencePath);
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
