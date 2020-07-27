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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions;

/**
 * Records a series of actions and can determine whether a subsequent action would "flip flop" with
 * the previously recorded actions.
 *
 * <p>A flip flop is defined as any action which would undo changes made by a previous action. For
 * example, decreasing CPU allocation then immediately increasing it may be considered a flip flop,
 * but this is up to the implementation.
 */
public interface FlipFlopDetector {

    /**
     * Determines whether action will "flip flop" with any of the previously recorded actions
     * @param action The {@link Action} to test
     * @return True if action will "flip flop" with any of the previously recorded actions
     */
    public boolean isFlipFlop(Action action);

    /**
     * Records that an action was applied. This action may then be used in any subsequent isFlipFlop
     * tests
     * @param action The action to record
     */
    public void recordAction(Action action);
}
