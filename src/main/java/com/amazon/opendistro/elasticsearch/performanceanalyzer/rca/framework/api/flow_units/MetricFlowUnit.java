/*
 * Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 *  permissions and limitations under the License.
 */

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.flow_units;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.FlowUnitMessage;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.StringList;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.GenericFlowUnit;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.persistence.FlowUnitWrapper;
import java.util.List;

public class MetricFlowUnit extends GenericFlowUnit {
  public MetricFlowUnit(long timeStamp) {
    super(timeStamp);
  }

  public MetricFlowUnit(long timeStamp, List<List<String>> data) {
    super(timeStamp, data);
  }

  public static MetricFlowUnit generic() {
    return new MetricFlowUnit(System.currentTimeMillis());
  }

  public FlowUnitMessage buildFlowUnitMessage(final String graphNode, final String esNode) {
    final FlowUnitMessage.Builder messageBuilder = FlowUnitMessage.newBuilder();
    messageBuilder.setGraphNode(graphNode);
    messageBuilder.setEsNode(esNode);

    if (!this.isEmpty()) {
      for (List<String> value : this.getData()) {
        messageBuilder.addValues(StringList.newBuilder().addAllValues(value).build());
      }
    }
    messageBuilder.setTimestamp(System.currentTimeMillis());

    return messageBuilder.build();
  }

  /*
      metric table will look something similar to :
      [[MemType, sum, avg, min, max],
      [Eden, 8.6555376E7, 8.6555376E7, 8.6555376E7, 8.6555376E7],
      [Heap, 5.4237588E8, 5.4237588E8, 5.4237588E8, 5.4237588E8],

      So here we have :
      columnTypeName => "MemType", rowName => "Heap", columnName => "Max"
  */
  public double getDataFromMetric(String columnTypeName, String rowName, String columnName) {
    if (this.isEmpty() || this.getData().isEmpty()) {
      return Double.NaN;
    }
    int colIdx = -1;
    int rowIdx = -1;
    int colTypeIdx = -1;
    List<String> cols = this.getData().get(0);
    // Get the index of the MemType column and index of the max column.
    for (int i = 0; i < cols.size(); i++) {
      if (cols.get(i).equals(columnTypeName)) {
        colTypeIdx = i;
      } else if (cols.get(i).equals(columnName)) {
        colIdx = i;
      }
      if (colTypeIdx != -1 && colIdx != -1) {
        break;
      }
    }
    if (colTypeIdx == -1 || colIdx == -1) {
      return Double.NaN;
    }
    // The first row is the column names, so we start from the row 1.
    for (int i = 1; i < this.getData().size(); i++) {
      List<String> row = this.getData().get(i);
      String colType = row.get(colTypeIdx);
      if (colType.equals(rowName)) {
        rowIdx = i;
        break;
      }
    }
    if (rowIdx == -1) {
      return Double.NaN;
    }
    return Double.parseDouble(this.getData().get(rowIdx).get(colIdx));
  }

  public static MetricFlowUnit buildFlowUnitFromWrapper(final FlowUnitWrapper value) {
    if (value.hasData()) {
      return new MetricFlowUnit(value.getTimeStamp(), value.getData());
    } else {
      return new MetricFlowUnit(value.getTimeStamp());
    }
  }

  @Override
  public String toString() {
    return String.format("%d: %s", this.getTimeStamp(), this.getData());
  }
}
