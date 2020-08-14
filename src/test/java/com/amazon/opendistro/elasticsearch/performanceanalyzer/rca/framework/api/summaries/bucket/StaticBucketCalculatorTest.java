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

public class StaticBucketCalculatorTest extends BucketCalculatorTest {

  @Override
  void initUut() {
    uut = new StaticBucketCalculator(10.0, 40.0, 70.0);
  }

  @Override
  public void verifyCompute() {
    Assert.assertEquals(UsageBucket.HEALTHY_WITH_BUFFER, uut.compute(ResourceEnum.CPU, -10.0));
    Assert.assertEquals(UsageBucket.HEALTHY_WITH_BUFFER, uut.compute(ResourceEnum.CPU, 10.0));
    Assert.assertEquals(UsageBucket.HEALTHY, uut.compute(ResourceEnum.CPU, 40.0));
    Assert.assertEquals(UsageBucket.UNDER_PROVISIONED, uut.compute(ResourceEnum.CPU, 70.0));
    Assert.assertEquals(UsageBucket.UNHEALTHY, uut.compute(ResourceEnum.CPU, 70.1));
    Assert.assertEquals(UsageBucket.UNHEALTHY, uut.compute(ResourceEnum.CPU, 10000));
  }
}
