package com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.deciders;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.Action;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.deciders.collator.Collator;
import com.google.common.collect.Lists;

import java.time.Instant;
import java.util.List;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

public class PublisherTest {
    private static final int EVAL_INTERVAL_S = 5;
    private static Publisher publisher;

    // Mock objects
    @Mock
    private Collator collator;

    @Mock
    private Decision decision;

    @Mock
    private Action action;

    private static class TestDecider extends Decider {
        public TestDecider(long evalIntervalSeconds, int decisionFrequency) {
            super(evalIntervalSeconds, decisionFrequency);
        }

        @Override
        public String name() {
            return getClass().getSimpleName();
        }

        @Override
        public Decision operate() {
            return null;
        }
    }

    @BeforeClass
    public static void setupClass() {
    }

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
        publisher = new Publisher(EVAL_INTERVAL_S, collator);
    }

    @Test
    public void testIsCooledOff() throws Exception {
        List<Decision> decisionList = Lists.newArrayList(decision);
        Mockito.when(collator.getFlowUnits()).thenReturn(decisionList);
        Mockito.when(decision.getActions()).thenReturn(Lists.newArrayList(action));
        Mockito.when(action.name()).thenReturn("testIsCooledOffAction");
        Mockito.when(action.coolOffPeriodInMillis()).thenReturn(100_000L);
        // Verify that a newly initialized publisher doesn't execute an action until the publisher object
        // has been alive for longer than the action's cool off period
        publisher.operate();
        Mockito.verify(action, Mockito.times(0)).execute();
        Mockito.when(action.coolOffPeriodInMillis()).thenReturn(Instant.now().toEpochMilli()
                - publisher.getInitTime() - 1000L);
        publisher.operate();
        Mockito.verify(action, Mockito.times(1)).execute();
        Mockito.reset(action);
        // Verify that a publisher doesn't execute a previously executed action until the action's cool off period
        // has elapsed
        Mockito.when(action.coolOffPeriodInMillis()).thenReturn(3000L);
        publisher.operate();
        Mockito.verify(action, Mockito.times(0)).execute();
        // Verify that a published executes a previously executed action once the action's cool off period has elapsed
        Thread.sleep(4000L);
        publisher.operate();
        Mockito.verify(action, Mockito.times(1)).execute();
    }
}
