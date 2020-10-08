package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.persistence;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.Action;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.ImpactVector;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.util.InstanceDetails;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.persistence.actions.PersistedAction;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.cluster.NodeKey;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class PublisherEventsPersistorTest {
    private Path testLocation = null;
    private final String baseFilename = "rca.test.file";

    private Persistable persistable;
    private PublisherEventsPersistor publisherEventsPersistor;

    @Before
    public void init() throws Exception {
        String cwd = System.getProperty("user.dir");
        testLocation = Paths.get(cwd, "src", "test", "resources", "tmp", "file_rotate");
        Files.createDirectories(testLocation);
        FileUtils.cleanDirectory(testLocation.toFile());

        persistable = new SQLitePersistor(
                testLocation.toString(), baseFilename, String.valueOf(1), TimeUnit.SECONDS, 1);
        publisherEventsPersistor = new PublisherEventsPersistor(persistable);
    }

    @After
    public void cleanup() throws IOException {
        FileUtils.cleanDirectory(testLocation.toFile());
    }

    @Test
    public void actionPublished()
            throws InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException, InterruptedException {
        final MockAction mockAction1 = new MockAction("MockAction1");
        final MockAction mockAction2 = new MockAction("MockAction2");

        List<Action> mockActions = new ArrayList<>();
        mockActions.add(mockAction1);
        mockActions.add(mockAction2);

        publisherEventsPersistor.persistAction(mockActions);

        List<PersistedAction> actionsSummary = persistable.readLatestGroup(PersistedAction.class);
        Assert.assertNotNull(actionsSummary);
        Assert.assertEquals(actionsSummary.size(), 2);
        int index = 1;
        for (PersistedAction action : actionsSummary) {
            Assert.assertEquals(action.getActionName(), "MockAction" + index++);
            Assert.assertEquals(action.getNodeIds(), "{1,2}");
            Assert.assertEquals(action.getNodeIps(), "{1.1.1.1,2.2.2.2}");
            Assert.assertEquals(action.isActionable(), mockAction2.isActionable());
            Assert.assertEquals(action.getCoolOffPeriod(), mockAction2.coolOffPeriodInMillis());
            Assert.assertEquals(action.isMuted(), mockAction2.isMuted());
            Assert.assertEquals(action.getSummary(), mockAction2.summary());
        }

        Thread.sleep(1);

        final MockAction mockAction3 = new MockAction("MockAction3");
        final MockAction mockAction4 = new MockAction("MockAction4");
        mockActions.clear();
        mockActions.add(mockAction3);
        mockActions.add(mockAction4);
        publisherEventsPersistor.persistAction(mockActions);

        actionsSummary = persistable.readLatestGroup(PersistedAction.class);
        Assert.assertNotNull(actionsSummary);
        Assert.assertEquals(actionsSummary.size(), 2);
        index = 3;
        for (PersistedAction action : actionsSummary) {
            Assert.assertEquals(action.getActionName(), "MockAction" + index++);
            Assert.assertEquals(action.getNodeIds(), "{1,2}");
            Assert.assertEquals(action.getNodeIps(), "{1.1.1.1,2.2.2.2}");
            Assert.assertEquals(action.isActionable(), mockAction3.isActionable());
            Assert.assertEquals(action.getCoolOffPeriod(), mockAction3.coolOffPeriodInMillis());
            Assert.assertEquals(action.isMuted(), mockAction3.isMuted());
            Assert.assertEquals(action.getSummary(), mockAction3.summary());
        }

    }

    public class MockAction implements Action {
        private String name;

        public MockAction(String name) {
            this.name = name;
        }

        @Override
        public boolean isActionable() {
            return false;
        }

        @Override
        public long coolOffPeriodInMillis() {
            return 0;
        }

        @Override
        public List<NodeKey> impactedNodes() {
            List<NodeKey> nodeKeys = new ArrayList<>();
            nodeKeys.add(new NodeKey(new InstanceDetails.Id("1"), new InstanceDetails.Ip("1.1.1.1")));
            nodeKeys.add(new NodeKey(new InstanceDetails.Id("2"), new InstanceDetails.Ip("2.2.2.2")));
            return nodeKeys;
        }

        @Override
        public Map<NodeKey, ImpactVector> impact() {
            return null;
        }

        @Override
        public String name() {
            return this.name;
        }

        @Override
        public String summary() {
            return "MockSummary";
        }

        @Override
        public boolean isMuted() {
            return false;
        }
    }
}
