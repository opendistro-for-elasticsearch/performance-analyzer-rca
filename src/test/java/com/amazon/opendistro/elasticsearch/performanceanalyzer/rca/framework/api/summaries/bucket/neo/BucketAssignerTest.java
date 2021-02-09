/*
 * Copyright 2021 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.bucket.neo;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.bucket.BucketCalculator;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.bucket.UsageBucket;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.RcaConf;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.util.RcaConsts;
import com.fasterxml.jackson.core.JsonProcessingException;
import java.nio.file.Paths;
import org.junit.Assert;
import org.junit.Test;

public class BucketAssignerTest {
  private static final String configStr =
      "{  "
          + "\"bucketization\": {"
          + "\"base1\": {"
          + "\"UNDER_UTILIZED\": 20.0,"
          + "\"HEALTHY_WITH_BUFFER\": 40.0,"
          + "\"HEALTHY\": 80.0},"
          + "\"base2\": {"
          + "\"UNDER_UTILIZED\": 30.0,"
          + "\"HEALTHY_WITH_BUFFER\": 40.0,"
          + "\"HEALTHY\": 75.0},"
          + "\"base3\": {"
          + "\"UNDER_UTILIZED\": 30.1,"
          + "\"HEALTHY_WITH_BUFFER\": 40.23456,"
          + "\"HEALTHY\": 75.0"
          + "}"
          + "}"
          + "}";

  @Test
  public void testCreateBucketAssigner() throws JsonProcessingException {
    RcaConf rcaConf = new RcaConf();
    rcaConf.readConfigFromString(configStr);

    BucketCalculator bucketCalculatorForBase1 = rcaConf.getBucketizationSettings("base1");

    Assert.assertEquals(UsageBucket.UNDER_UTILIZED, bucketCalculatorForBase1.compute(19));
    Assert.assertEquals(UsageBucket.HEALTHY_WITH_BUFFER, bucketCalculatorForBase1.compute(40));
    Assert.assertEquals(UsageBucket.HEALTHY, bucketCalculatorForBase1.compute(80));
    Assert.assertEquals(UsageBucket.UNHEALTHY, bucketCalculatorForBase1.compute(81));

    BucketCalculator bucketCalculatorForBase3 = rcaConf.getBucketizationSettings("base3");

    Assert.assertEquals(UsageBucket.UNDER_UTILIZED, bucketCalculatorForBase3.compute(30));
    Assert.assertEquals(UsageBucket.HEALTHY_WITH_BUFFER, bucketCalculatorForBase3.compute(40.1));
    Assert.assertEquals(UsageBucket.HEALTHY, bucketCalculatorForBase3.compute(75));
    Assert.assertEquals(UsageBucket.UNHEALTHY, bucketCalculatorForBase3.compute(76));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testMisSpelledTunableName() throws JsonProcessingException {
    RcaConf rcaConf = new RcaConf();
    rcaConf.readConfigFromString(configStr);
    rcaConf.getBucketizationSettings("basa3");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testNoBucketizationSettings() {
    RcaConf rcaConf = new RcaConf(Paths.get(RcaConsts.TEST_CONFIG_PATH, "rca.conf").toString());
    rcaConf.getBucketizationSettings("base1");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testNoThresholds() throws JsonProcessingException {
    final String configStr = "{\"bucketization\": {}}";
    RcaConf rcaConf = new RcaConf();
    rcaConf.readConfigFromString(configStr);
    rcaConf.getBucketizationSettings("base1");
  }
}
