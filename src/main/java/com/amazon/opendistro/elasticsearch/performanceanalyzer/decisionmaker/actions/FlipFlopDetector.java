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
