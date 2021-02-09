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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.flow_units;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.FlowUnitMessage;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.GenericFlowUnit;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.util.InstanceDetails;
import java.util.List;
import org.jooq.Field;
import org.jooq.Record;
import org.jooq.Result;

public class MetricFlowUnit extends GenericFlowUnit {

  private Result<Record> data = null;

  public MetricFlowUnit(long timeStamp) {
    super(timeStamp);
  }

  public MetricFlowUnit(long timeStamp, Result<Record> data) {
    super(timeStamp);
    this.data = data;
    this.empty = false;
  }

  /**
   * read SQL result from flowunit
   * @return SQL result
   */
  public Result<Record> getData() {
    return data;
  }

  public static MetricFlowUnit generic() {
    return new MetricFlowUnit(System.currentTimeMillis());
  }

  /**
   * Metric flowunit is not supposed be serialized and sent over to remote nodes. This function will
   * never be called. so return null in case we run into it.
   */
  @Override
  public FlowUnitMessage buildFlowUnitMessage(final String graphNode, final InstanceDetails.Id esNode) {
    return null;
  }

  @Override
  public String toString() {
    return String.format("%d: %s", this.getTimeStamp(), this.getData());
  }
}
