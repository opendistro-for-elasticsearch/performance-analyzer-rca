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

import com.amazon.opendistro.elasticsearch.performanceanalyzer.AppContext;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.RcaConf;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.cluster.NodeKey;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.annotations.SerializedName;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class AdmissionControlAction extends SuppressibleAction {

    public static final String NAME = "AdmissionControlAction";

    public static final long DEFAULT_COOL_OFF_PERIOD_IN_MILLIS = TimeUnit.MINUTES.toMillis(15);

    private final NodeKey esNode;
    private final String controllerName;
    private final boolean canUpdate;
    private final double desiredValue;
    private final double currentValue;

    public AdmissionControlAction(
            AppContext appContext,
            final NodeKey esNode,
            final String controllerName,
            final boolean canUpdate,
            final double desiredValue,
            final double currentValue) {
        super(appContext);
        this.esNode = esNode;
        this.canUpdate = canUpdate;
        this.desiredValue = desiredValue;
        this.currentValue = currentValue;
        this.controllerName = controllerName;
    }

    public static Builder newBuilder(
            final NodeKey esNode,
            final String controllerName,
            final AppContext appContext,
            final RcaConf conf) {
        return new Builder(esNode, controllerName, appContext, conf);
    }

    public String getControllerName() {
        return controllerName;
    }

    @Override
    public boolean canUpdate() {
        return this.canUpdate;
    }

    @Override
    public long coolOffPeriodInMillis() {
        return DEFAULT_COOL_OFF_PERIOD_IN_MILLIS;
    }

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public List<NodeKey> impactedNodes() {
        return Collections.singletonList(esNode);
    }

    @Override
    public Map<NodeKey, ImpactVector> impact() {
        // AdmissionControl tuning is bi-directional
        // We mark increase/decrease pressure based on desired value
        final ImpactVector impactVector = new ImpactVector();
        if (desiredValue > currentValue) {
            impactVector.increasesPressure(ADMISSION_CONTROL);
        } else if (desiredValue < currentValue) {
            impactVector.decreasesPressure(ADMISSION_CONTROL);
        }
        return Collections.singletonMap(esNode, impactVector);
    }

    @Override
    public String summary() {
        Summary summary =
                new Summary(
                        esNode.getNodeId().toString(),
                        esNode.getHostAddress().toString(),
                        desiredValue,
                        currentValue,
                        DEFAULT_COOL_OFF_PERIOD_IN_MILLIS,
                        canUpdate);
        return summary.toJson();
    }

    public double getCurrentValue() {
        return this.currentValue;
    }

    public double getDesiredValue() {
        return this.desiredValue;
    }

    public static final class Builder {
        private final String controllerName;
        private final NodeKey esNode;
        private final AppContext appContext;
        private final RcaConf rcaConf;
        private Double currentValue;
        private Double desiredValue;

        private Builder(
                final NodeKey esNode,
                final String controllerName,
                final AppContext appContext,
                final RcaConf conf) {
            this.esNode = esNode;
            this.controllerName = controllerName;
            this.appContext = appContext;
            this.rcaConf = conf;
        }

        public Builder currentValue(Double currentValue) {
            this.currentValue = currentValue;
            return this;
        }

        public Builder desiredValue(Double desiredValue) {
            this.desiredValue = desiredValue;
            return this;
        }

        public AdmissionControlAction build() {
            boolean canUpdate = desiredValue != 0;
            return new AdmissionControlAction(
                    appContext, esNode, controllerName, canUpdate, desiredValue, currentValue);
        }
    }

    public static class Summary {

        public static final String ID = "Id";
        public static final String IP = "Ip";
        public static final String DESIRED_VALUE = "desiredValue";
        public static final String CURRENT_VALUE = "currentValue";
        public static final String COOL_OFF_PERIOD = "coolOffPeriodInMillis";
        public static final String CAN_UPDATE = "canUpdate";

        @SerializedName(value = ID)
        private String id;

        @SerializedName(value = IP)
        private String ip;

        @SerializedName(value = DESIRED_VALUE)
        private double desiredValue;

        @SerializedName(value = CURRENT_VALUE)
        private double currentValue;

        @SerializedName(value = COOL_OFF_PERIOD)
        private long coolOffPeriodInMillis;

        @SerializedName(value = CAN_UPDATE)
        private boolean canUpdate;

        public Summary(
                String id,
                String ip,
                double desiredValue,
                double currentValue,
                long coolOffPeriodInMillis,
                boolean canUpdate) {
            this.id = id;
            this.ip = ip;
            this.desiredValue = desiredValue;
            this.currentValue = currentValue;
            this.coolOffPeriodInMillis = coolOffPeriodInMillis;
            this.canUpdate = canUpdate;
        }

        public String toJson() {
            Gson gson = new GsonBuilder().disableHtmlEscaping().create();
            return gson.toJson(this);
        }

        public String getId() {
            return this.id;
        }

        public String getIp() {
            return this.ip;
        }

        public double getCurrentValue() {
            return this.currentValue;
        }

        public double getDesiredValue() {
            return this.desiredValue;
        }

        public long getCoolOffPeriodInMillis() {
            return coolOffPeriodInMillis;
        }

        public boolean getCanUpdate() {
            return canUpdate;
        }
    }
}
