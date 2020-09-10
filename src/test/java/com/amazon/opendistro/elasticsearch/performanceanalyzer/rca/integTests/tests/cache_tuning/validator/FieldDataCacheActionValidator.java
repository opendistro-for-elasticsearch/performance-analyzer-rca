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
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.integTests.framework.api.IValidator;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.persistence.actions.ActionsSummary;
import org.junit.Assert;

public class FieldDataCacheActionValidator implements IValidator {
    long startTime;

    public FieldDataCacheActionValidator() {
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
        Assert.assertEquals(ResourceEnum.FIELD_DATA_CACHE.getNumber(), actionObject.getResourceValue());
        return true;
    }
}