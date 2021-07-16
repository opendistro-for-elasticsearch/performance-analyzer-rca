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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions;

import static com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.ImpactVector.Dimension.ADMISSION_CONTROL;
import static com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.ImpactVector.Dimension.CPU;
import static com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.ImpactVector.Dimension.DISK;
import static com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.ImpactVector.Dimension.HEAP;
import static com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.ImpactVector.Dimension.NETWORK;
import static com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.ImpactVector.Dimension.RAM;
import static com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.admissioncontrol.AdmissionControlRca.REQUEST_SIZE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.AppContext;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.RcaConf;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.util.InstanceDetails;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.util.RcaConsts;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.cluster.NodeKey;
import com.google.common.collect.ImmutableSet;
import java.nio.file.Paths;
import java.util.Map;
import org.junit.Assert;
import org.junit.Test;

public class AdmissionControlActionTest {

    private AppContext testAppContext;
    private RcaConf rcaConf;

    public AdmissionControlActionTest() {
        testAppContext = new AppContext();
        String rcaConfPath = Paths.get(RcaConsts.TEST_CONFIG_PATH, "rca.conf").toString();
        rcaConf = new RcaConf(rcaConfPath);
    }

    @Test
    public void testAdmissionControlActionIncreasePressure() {
        NodeKey node1 =
                new NodeKey(new InstanceDetails.Id("node-1"), new InstanceDetails.Ip("1.2.3.4"));
        AdmissionControlAction.Builder builder =
                AdmissionControlAction.newBuilder(node1, REQUEST_SIZE, testAppContext, rcaConf);
        AdmissionControlAction admissionControlAction =
                builder.currentValue(10.0).desiredValue(20.0).build();

        Assert.assertNotNull(admissionControlAction);
        assertTrue(admissionControlAction.isActionable());
        assertEquals(
                AdmissionControlAction.DEFAULT_COOL_OFF_PERIOD_IN_MILLIS,
                admissionControlAction.coolOffPeriodInMillis());
        assertEquals(REQUEST_SIZE, admissionControlAction.getControllerName());
        assertEquals(1, admissionControlAction.impactedNodes().size());

        Map<ImpactVector.Dimension, ImpactVector.Impact> impact =
                admissionControlAction.impact().get(node1).getImpact();
        assertEquals(ImpactVector.Impact.INCREASES_PRESSURE, impact.get(ADMISSION_CONTROL));
        assertEquals(ImpactVector.Impact.NO_IMPACT, impact.get(HEAP));
        assertEquals(ImpactVector.Impact.NO_IMPACT, impact.get(CPU));
        assertEquals(ImpactVector.Impact.NO_IMPACT, impact.get(RAM));
        assertEquals(ImpactVector.Impact.NO_IMPACT, impact.get(DISK));
        assertEquals(ImpactVector.Impact.NO_IMPACT, impact.get(NETWORK));
    }

    @Test
    public void testAdmissionControlActionDecreasePressure() {
        NodeKey node1 =
                new NodeKey(new InstanceDetails.Id("node-1"), new InstanceDetails.Ip("1.2.3.4"));
        AdmissionControlAction.Builder builder =
                AdmissionControlAction.newBuilder(node1, REQUEST_SIZE, testAppContext, rcaConf);
        AdmissionControlAction admissionControlAction =
                builder.currentValue(20.0).desiredValue(10.0).build();

        Assert.assertNotNull(admissionControlAction);
        assertTrue(admissionControlAction.isActionable());
        assertEquals(
                AdmissionControlAction.DEFAULT_COOL_OFF_PERIOD_IN_MILLIS,
                admissionControlAction.coolOffPeriodInMillis());
        assertEquals(REQUEST_SIZE, admissionControlAction.getControllerName());
        assertEquals(1, admissionControlAction.impactedNodes().size());

        Map<ImpactVector.Dimension, ImpactVector.Impact> impact =
                admissionControlAction.impact().get(node1).getImpact();
        assertEquals(ImpactVector.Impact.DECREASES_PRESSURE, impact.get(ADMISSION_CONTROL));
        assertEquals(ImpactVector.Impact.NO_IMPACT, impact.get(HEAP));
        assertEquals(ImpactVector.Impact.NO_IMPACT, impact.get(CPU));
        assertEquals(ImpactVector.Impact.NO_IMPACT, impact.get(RAM));
        assertEquals(ImpactVector.Impact.NO_IMPACT, impact.get(DISK));
        assertEquals(ImpactVector.Impact.NO_IMPACT, impact.get(NETWORK));
    }

    @Test
    public void testMutedAction() {
        NodeKey node1 =
                new NodeKey(new InstanceDetails.Id("node-1"), new InstanceDetails.Ip("1.2.3.4"));
        AdmissionControlAction.Builder builder =
                AdmissionControlAction.newBuilder(node1, REQUEST_SIZE, testAppContext, rcaConf);
        AdmissionControlAction admissionControlAction =
                builder.currentValue(10.0).desiredValue(20.0).build();
        testAppContext.updateMutedActions(ImmutableSet.of(admissionControlAction.name()));
        assertFalse(admissionControlAction.isActionable());
    }

    @Test
    public void testSummary() {
        NodeKey node1 =
                new NodeKey(new InstanceDetails.Id("node-1"), new InstanceDetails.Ip("1.2.3.4"));
        AdmissionControlAction.Builder builder =
                AdmissionControlAction.newBuilder(node1, REQUEST_SIZE, testAppContext, rcaConf);
        AdmissionControlAction admissionControlAction =
                builder.currentValue(10.0).desiredValue(20.0).build();
        String summary = admissionControlAction.summary();
        assertEquals(
                new StringBuilder()
                        .append("{")
                        .append("\"Id\":\"node-1\",")
                        .append("\"Ip\":\"1.2.3.4\",")
                        .append("\"desiredValue\":20.0,")
                        .append("\"currentValue\":10.0,")
                        .append("\"coolOffPeriodInMillis\":300000,")
                        .append("\"canUpdate\":true}")
                        .toString(),
                summary);
    }
}
