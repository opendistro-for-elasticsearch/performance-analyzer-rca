/*
 * Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.stats.eval.impl;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.stats.eval.Statistics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.stats.eval.impl.vals.Value;
import java.util.Collections;
import java.util.List;

public class Sample implements IStatistic<Value> {
  private Number value;
  private boolean empty;

  public Sample() {
    empty = true;
  }

  @Override
  public Statistics type() {
    return Statistics.SAMPLE;
  }

  @Override
  public void calculate(String key, Number value) {
    this.value = value;
    empty = false;
  }

  @Override
  public List<Value> get() {
    return Collections.singletonList(new Value(value));
  }

  @Override
  public boolean isEmpty() {
    return empty;
  }
}
