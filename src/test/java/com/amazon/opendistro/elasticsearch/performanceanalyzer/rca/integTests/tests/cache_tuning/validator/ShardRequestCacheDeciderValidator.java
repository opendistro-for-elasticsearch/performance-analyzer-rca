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
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.integTests.framework.api.IValidator;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.persistence.actions.PersistedAction;
import org.junit.Assert;

public class ShardRequestCacheDeciderValidator implements IValidator {
    long startTime;

    public ShardRequestCacheDeciderValidator() {
        startTime = System.currentTimeMillis();
    }

    /**
     * {"actionName":"ModifyCacheMaxSize",
     * "resourceValue":11,
     * "timestamp":"1599257910923",
     * "nodeId":"node1",
     * "nodeIp":1.1.1.1,
     * "actionable":1,
     * "coolOffPeriod": 300000,
     * "muted": 1,
     * "summary": Update [SHARD_REQUEST_CACHE] capacity from [10000] to [100000] on node [DATA_0]
     */
    @Override
    public <T> boolean check(T response) {
        if (response == null) {
            return false;
        }
        PersistedAction persistedAction = (PersistedAction) response;
        return checkPersistedAction(persistedAction);
    }

    /**
     * {"actionName":"ModifyCacheMaxSize",
     * "resourceValue":11,
     * "timestamp":"1599257910923",
     * "nodeId":"node1",
     * "nodeIp":1.1.1.1,
     * "actionable":1,
     * "coolOffPeriod": 300000,
     * "muted": 1
     * "summary": Update [SHARD_REQUEST_CACHE] capacity from [10000] to [100000] on node [DATA_0]
     */
    private boolean checkPersistedAction(final PersistedAction persistedAction) {
        Assert.assertEquals(ModifyCacheMaxSizeAction.NAME, persistedAction.getActionName());
        Assert.assertEquals("{DATA_0}", persistedAction.getNodeIds());
        Assert.assertEquals("{127.0.0.1}", persistedAction.getNodeIps());
        Assert.assertEquals(300000, persistedAction.getCoolOffPeriod());
        Assert.assertTrue(persistedAction.isActionable());
        Assert.assertFalse(persistedAction.isMuted());
        return true;
    }
}
