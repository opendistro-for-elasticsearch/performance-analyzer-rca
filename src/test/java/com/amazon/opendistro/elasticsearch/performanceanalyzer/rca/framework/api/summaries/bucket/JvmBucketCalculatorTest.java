/*
 * Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.bucket;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.ResourceEnum;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class JvmBucketCalculatorTest {
  JvmBucketCalculator uut;

  @Before
  public void setup() {
    StaticBucketCalculator youngGenCalc = new StaticBucketCalculator(10.0, 40.0, 70.0);
    StaticBucketCalculator oldGenCalc = new StaticBucketCalculator(5.0, 7.0, 20.0);
    uut = new JvmBucketCalculator(youngGenCalc, oldGenCalc);
  }

  @Test
  public void testCompute() {
    ResourceEnum youngGen = ResourceEnum.YOUNG_GEN;
    ResourceEnum oldGen = ResourceEnum.OLD_GEN;
    // YOUNG GEN
    Assert.assertEquals(UsageBucket.HEALTHY_WITH_BUFFER, uut.compute(youngGen, -10.0));
    Assert.assertEquals(UsageBucket.HEALTHY_WITH_BUFFER, uut.compute(youngGen, 10.0));
    Assert.assertEquals(UsageBucket.HEALTHY, uut.compute(youngGen, 40.0));
    Assert.assertEquals(UsageBucket.UNDER_UTILIZED, uut.compute(youngGen, 70.0));
    Assert.assertEquals(UsageBucket.UNHEALTHY, uut.compute(youngGen, 70.1));
    Assert.assertEquals(UsageBucket.UNHEALTHY, uut.compute(youngGen, 10000));
    // OLD GEN
    Assert.assertEquals(UsageBucket.HEALTHY_WITH_BUFFER, uut.compute(oldGen, -5.0));
    Assert.assertEquals(UsageBucket.HEALTHY_WITH_BUFFER, uut.compute(oldGen, 5.0));
    Assert.assertEquals(UsageBucket.HEALTHY, uut.compute(oldGen, 5.1));
    Assert.assertEquals(UsageBucket.UNDER_UTILIZED, uut.compute(oldGen, 7.1));
    Assert.assertEquals(UsageBucket.UNHEALTHY, uut.compute(oldGen, 20.1));
    Assert.assertEquals(UsageBucket.UNHEALTHY, uut.compute(oldGen, 10000));
  }
}
