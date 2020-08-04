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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries;

/**
 * AnyNodeSummary is a HotClusterSummary which may contain a summary of healthy values
 */
public class AnyClusterSummary extends HotClusterSummary {
  private boolean isHot;

  public AnyClusterSummary(int numOfNodes, int numOfUnhealthyNodes, boolean isHot) {
    super(numOfNodes, numOfUnhealthyNodes);
    this.isHot = isHot;
  }

  public boolean isHot() {
    return isHot;
  }

  public void setHot(boolean hot) {
    isHot = hot;
  }
}
