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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.deciders;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.Action;
import java.util.List;

/**
 * A DecisionPolicy evaluates a subset of observation summaries for
 * a Decider, and returns a list of recommended Actions. They abstract out a subset
 * of the decision making process for a decider.
 *
 * <p> </p>Decision policies are invoked by deciders and never scheduled directly by the RCA framework.
 */
public interface DecisionPolicy {

  List<Action> evaluate();

}
