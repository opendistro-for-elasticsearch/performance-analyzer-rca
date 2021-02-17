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

import com.amazon.opendistro.elasticsearch.performanceanalyzer.AppContext;

/**
 * Actions that can be suppressed through a config.
 */
public abstract class SuppressibleAction implements Action {

  protected final AppContext appContext;

  public SuppressibleAction(final AppContext appContext) {
    this.appContext = appContext;
  }

  /**
   * Returns if this action is explicitly muted through configuration
   */
  @Override
  public boolean isMuted() {
    return appContext.isActionMuted(name());
  }

  /**
   * Returns true if the configured action is actionable, false otherwise.
   * The method is also declared final to enforce checking for muted-ness of an action before
   * declaring it actionable.
   *
   * <p>Actions that want to define the actionable criterion differently should not inherit from
   * {@link SuppressibleAction}, instead provide their own implementation. This way, we get to
   * close {@link SuppressibleAction} while still keeping the {@link Action} interface open for
   * modification.</p>
   *
   * <p>Examples of non-actionable actions are invalid actions or actions that are explicitly
   * disabled in the conf file.
   */
  @Override
  public final boolean isActionable() {
    return !isMuted() && canUpdate();
  }

  /**
   * Returns true if the configured action is valid, false otherwise.
   *
   * <p>Examples of invalid actions are resource configurations where limits have been
   * reached.</p>
   *
   * @return true if valid, false otherwise.
   */
  public abstract boolean canUpdate();
}
