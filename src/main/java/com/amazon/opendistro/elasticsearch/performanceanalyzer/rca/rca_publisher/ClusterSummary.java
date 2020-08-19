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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.rca_publisher;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.FlowUnitMessage;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.GenericFlowUnit;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.GenericSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.util.InstanceDetails;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ClusterSummary <T extends GenericSummary> extends GenericFlowUnit {
    private String NAME = "ClusterRcaSummary";
    private Map<String, T> summaryMap;

    public ClusterSummary(long timestamp, Map<String, T> summaryMap){
        super(timestamp);
        setSummaryMap(summaryMap);
    }

    public boolean summaryMapIsEmpty(){
        return summaryMap.isEmpty();
    }

    public Map<String, T> getSummaryMap(){
        return summaryMap;
    }

    public void setSummaryMap(Map<String, T> summaryMap){
        this.summaryMap = summaryMap;
    }

    public String name(){
        return NAME;
    }

    public List<String> getExistingClusterNameList(){
        if(!summaryMapIsEmpty()){
            return new ArrayList<>(summaryMap.keySet());

        }
        return new ArrayList<>();
    }

    /** Returns a summary for the configured action */
    public T getSummary(String name){
        return summaryMap.get(name);
    }

    public void addSummary(String name, T summary){
        summaryMap.put(name, summary);
    }

    @Override
    public FlowUnitMessage buildFlowUnitMessage(String graphNode, InstanceDetails.Id esNode) {
        return null;
    }
}
