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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.integTests.tests.jvm.old_gen_policy.validator;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.ResourceEnum;
import com.google.gson.JsonArray;

public class LevelOneValidator extends OldGenPolicyBaseValidator {

  @Override
  public boolean checkPersistedActions(JsonArray actionJsonArray) {
    return (checkModifyCacheAction(actionJsonArray, ResourceEnum.FIELD_DATA_CACHE)
        && checkModifyCacheAction(actionJsonArray, ResourceEnum.SHARD_REQUEST_CACHE));
  }
}