/*
 * Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.configs;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.RcaConf;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.util.RcaConsts;
import java.nio.file.Paths;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class QueueRejectionRcaConfigTest {

  private RcaConf rcaConf;
  private RcaConf rcaOldConf;

  @Before
  public void init() {
    String rcaConfPath = Paths.get(RcaConsts.TEST_CONFIG_PATH, "rca_elected_master.conf").toString();
    String rcaOldConfPath = Paths.get(RcaConsts.TEST_CONFIG_PATH, "rca_master.conf").toString();
    rcaConf = new RcaConf(rcaConfPath);
    rcaOldConf = new RcaConf(rcaOldConfPath);
  }

  @Test
  public void testReadConfig() {
    QueueRejectionRcaConfig config = new QueueRejectionRcaConfig(rcaConf);
    Assert.assertEquals(400, config.getRejectionTimePeriodInSeconds());

    QueueRejectionRcaConfig oldConfig = new QueueRejectionRcaConfig(rcaOldConf);
    Assert.assertEquals(QueueRejectionRcaConfig.DEFAULT_REJECTION_TIME_PERIOD_IN_SECONDS,
        oldConfig.getRejectionTimePeriodInSeconds());
  }
}
