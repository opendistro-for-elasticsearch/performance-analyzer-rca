/*
 *  Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License").
 *  You may not use this file except in compliance with the License.
 *  A copy of the License is located at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  or in the "license" file accompanying this file. This file is distributed
 *  on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *  express or implied. See the License for the specific language governing
 *  permissions and limitations under the License.
 */

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.aggregators;

/**
 * SlidingWindowData holds a timestamp in ms and a double value. It is the basic datatype used by a
 * {@link SlidingWindow}
 */
public class SlidingWindowData {
  protected long timeStamp;
  protected double value;

  public SlidingWindowData() {
    this.timeStamp = -1;
    this.value = -1;
  }

  public SlidingWindowData(long timeStamp, double value) {
    this.timeStamp = timeStamp;
    this.value = value;
  }

  public long getTimeStamp() {
    return this.timeStamp;
  }

  public void setTimeStamp(long timeStamp) {
    this.timeStamp = timeStamp;
  }

  public double getValue() {
    return this.value;
  }

  public void setValue(double value) {
    this.value = value;
  }
}



