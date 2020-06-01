/*
 * Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 *  A copy of the License is located at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  or in the "license" file accompanying this file. This file is distributed
 *  on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *  express or implied. See the License for the specific language governing
 *  permissions and limitations under the License.
 */

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.temperature;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.temperature.HeatZoneAssigner;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.temperature.TemperatureDimension;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.temperature.TemperatureVector;
import java.util.ArrayList;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;

public class NodeLevelDimensionalSummaryTest {
  @Test
  public void getShardsInReverseTemperatureOrder() {
    final HeatZoneAssigner.Zone ZONE = HeatZoneAssigner.Zone.HOT;
    final TemperatureDimension DIMENSION = TemperatureDimension.CPU_Utilization;
    final int SHARD_COUNT = 10;

    NodeLevelDimensionalSummary nodeSummary =
        new NodeLevelDimensionalSummary(DIMENSION, new TemperatureVector.NormalizedValue((short) 2),
                12.0,20);

    // The list of shards so obtained is shards ordered in ascending order of temperature
    // along the Dimension = DIMENSION.
    for (ShardProfileSummary shard : getShards(DIMENSION, SHARD_COUNT)) {
      nodeSummary.addShardToZone(shard, ZONE);
    }

    List<ShardProfileSummary> shards = nodeSummary.getShardsForZoneInReverseTemperatureOrder(HeatZoneAssigner.Zone.HOT);

    // Although the shards were inserted in the ascending order of temperature,
    // the getShardsForZoneInReverseTemperatureOrder should return them in the
    // descending order of temperature.
    for (int i = 0; i < shards.size(); i++) {
      ShardProfileSummary shard = shards.get(i);
      Assert.assertEquals(SHARD_COUNT - i - 1, shard.getHeatInDimension(DIMENSION).getPOINTS());
    }
  }

  private List<ShardProfileSummary> getShards(TemperatureDimension dimension, int count) {
    List<ShardProfileSummary> shards = new ArrayList<>();

    for (int i = 0; i < count; i++) {
      ShardProfileSummary shard = new ShardProfileSummary("test-index", i);
      shard.addTemperatureForDimension(dimension,
          new TemperatureVector.NormalizedValue((short) i));
      shards.add(shard);
    }
    return shards;
  }
}