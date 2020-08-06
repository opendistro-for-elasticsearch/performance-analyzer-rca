package com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions;

/**
 * Actions that can be suppressed through a config.
 */
public abstract class SuppressibleAction implements Action {

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
    return !isMuted() && isValid();
  }

  /**
   * Returns true if the configured action is valid, false otherwise.
   *
   * <p>Examples of invalid actions are resource configurations where limits have been
   * reached.</p>
   *
   * @return true if valid, false otherwise.
   */
  public abstract boolean isValid();
}
