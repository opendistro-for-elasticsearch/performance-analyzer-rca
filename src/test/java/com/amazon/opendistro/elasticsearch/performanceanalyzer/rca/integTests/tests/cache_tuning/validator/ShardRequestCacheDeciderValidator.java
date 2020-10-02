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

import com.amazon.opendistro.elasticsearch.performanceanalyzer.AppContext;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.ModifyCacheMaxSizeAction;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.ResourceEnum;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.integTests.framework.api.IValidator;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.persistence.actions.PersistedAction;
import org.junit.Assert;

public class ShardRequestCacheDeciderValidator implements IValidator {
    AppContext appContext;
    long startTime;

    public ShardRequestCacheDeciderValidator() {
        appContext = new AppContext();
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
     * "summary": "Id":"DATA_0","Ip":"127.0.0.1","resource":11,"desiredCacheMaxSizeInBytes":10000,"currentCacheMaxSizeInBytes":100,
     *            "coolOffPeriodInMillis":300000,"canUpdate":true}
     */
    @Override
    public boolean checkDbObj(Object object) {
        if (object == null) {
            return false;
        }
        PersistedAction persistedAction = (PersistedAction) object;
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
     * "summary": "Id":"DATA_0","Ip":"127.0.0.1","resource":11,"desiredCacheMaxSizeInBytes":10000,"currentCacheMaxSizeInBytes":100,
     *            "coolOffPeriodInMillis":300000,"canUpdate":true}
     */
    private boolean checkPersistedAction(final PersistedAction persistedAction) {
        ModifyCacheMaxSizeAction modifyCacheMaxSizeAction =
                ModifyCacheMaxSizeAction.fromSummary(persistedAction.getSummary(), appContext);
        Assert.assertEquals(ModifyCacheMaxSizeAction.NAME, persistedAction.getActionName());
        Assert.assertEquals("{DATA_0}", persistedAction.getNodeIds());
        Assert.assertEquals("{127.0.0.1}", persistedAction.getNodeIps());
        Assert.assertEquals(300000, persistedAction.getCoolOffPeriod());
        Assert.assertTrue(persistedAction.isActionable());
        Assert.assertFalse(persistedAction.isMuted());
        Assert.assertEquals(ResourceEnum.SHARD_REQUEST_CACHE, modifyCacheMaxSizeAction.getCacheType());
        Assert.assertEquals(100, modifyCacheMaxSizeAction.getCurrentCacheMaxSizeInBytes());
        Assert.assertEquals(10000, modifyCacheMaxSizeAction.getDesiredCacheMaxSizeInBytes());
        return true;
    }
}
