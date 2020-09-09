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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.integTests.tests.cache_tuning.validator;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.ModifyCacheMaxSizeAction;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.ResourceEnum;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotClusterSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotNodeSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotResourceSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.integTests.framework.api.IValidator;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.integTests.tests.util.JsonParserUtil;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.persistence.actions.ActionsSummary;
import com.google.gson.JsonObject;
import org.junit.Assert;

import static com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotNodeSummary.SQL_SCHEMA_CONSTANTS.HOST_IP_ADDRESS_COL_NAME;
import static com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotNodeSummary.SQL_SCHEMA_CONSTANTS.NODE_ID_COL_NAME;
import static com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotResourceSummary.SQL_SCHEMA_CONSTANTS.RESOURCE_TYPE_COL_NAME;

public class ShardRequestCacheActionValidator implements IValidator {
    long startTime;

    public ShardRequestCacheActionValidator() {
        startTime = System.currentTimeMillis();
    }

    // TODO: Update comments
    @Override
    public <T> boolean check(T response) {
        if (response == null) {
            return false;
        }
        ActionsSummary actionsSummary = (ActionsSummary) response;
        return checkActionSummary(actionsSummary);
    }

    // TODO: Update comments
    private boolean checkActionSummary(final ActionsSummary actionObject) {
        Assert.assertEquals(ModifyCacheMaxSizeAction.NAME, actionObject.getActionName());
        Assert.assertEquals(ResourceEnum.SHARD_REQUEST_CACHE.getNumber(), actionObject.getResourceValue());
        return true;
    }
}