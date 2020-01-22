/*
 *  Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.stats.eval.impl;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.stats.eval.Statistics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.stats.eval.impl.vals.AggregateValue;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class Sum implements StatisticImpl<AggregateValue> {
  private AtomicLong sum;
  private boolean empty;

  public Sum() {
    sum = new AtomicLong(0L);
    empty = true;
  }

  @Override
  public Statistics type() {
    return Statistics.SUM;
  }

  @Override
  public void calculate(String key, Number value) {
    sum.addAndGet(value.longValue());
    empty = false;
  }

  @Override
  public List<AggregateValue> get() {
    return Collections.singletonList(new AggregateValue(sum, type()));
  }

  @Override
  public boolean isEmpty() {
    return empty;
  }
}
