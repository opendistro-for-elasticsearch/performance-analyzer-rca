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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.sys;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.stats.emitters.ISampler;
import java.util.Arrays;
import java.util.List;

public class AllJvmSamplers {
  public static List<ISampler> getJvmSamplers() {
    return Arrays.asList(new JvmFreeMem(), new JvmTotalMem());
  }
}
