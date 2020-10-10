package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.persistence;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.Action;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.ImpactVector;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.util.InstanceDetails;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.persistence.actions.PersistedAction;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.cluster.NodeKey;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.util.WaitFor;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.FileUtils;
import org.jooq.impl.DSL;
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
    public void actionPublished() throws Exception {
        final MockAction mockAction1 = new MockAction("MockAction1", new ArrayList<String>() {
            {
                add("1");
                add("11");
            }});
        final MockAction mockAction2 = new MockAction("MockAction2", new ArrayList<String>() {
            {
                add("2");
                add("22");
            }});
        List<Action> mockActions = new ArrayList<>();
        mockActions.add(mockAction1);
        mockActions.add(mockAction2);
        publisherEventsPersistor.persistAction(mockActions,123456789);

        mockActions.clear();
        final MockAction mockAction3 = new MockAction("MockAction3",new ArrayList<String>() {
            {
                add("3");
                add("33");
            }});
        final MockAction mockAction4 = new MockAction("MockAction4",new ArrayList<String>() {
            {
                add("4");
                add("44");
            }});
        mockActions.add(mockAction3);
        mockActions.add(mockAction4);

        publisherEventsPersistor.persistAction(mockActions, 987654321);

        WaitFor.waitFor(() -> persistable.readAllForMaxField(PersistedAction.class,
                DSL.field(PersistedAction.SQL_SCHEMA_CONSTANTS.TIMESTAMP_COL_NAME, String.class)
                ).size() == 2, 5,
                TimeUnit.SECONDS);
        List<PersistedAction> actionsSummary = persistable.readAllForMaxField(PersistedAction.class,
                DSL.field(PersistedAction.SQL_SCHEMA_CONSTANTS.TIMESTAMP_COL_NAME, String.class));
        Assert.assertNotNull(actionsSummary);
        Assert.assertEquals(actionsSummary.size(), 2);
        int index = 3;
        for (PersistedAction action : actionsSummary) {
            Assert.assertEquals(action.getActionName(), "MockAction" + index);
            String IP1 = (index + ".").repeat(4);
            String IP2 = (Integer.toString(index) + index + ".").repeat(4);
            Assert.assertEquals(action.getNodeIps(), IP1.substring(0, IP1.length() - 1) + ","
                                                         + IP2.substring(0, IP2.length() - 1));
            Assert.assertEquals(action.isActionable(), mockAction3.isActionable());
            Assert.assertEquals(action.getCoolOffPeriod(), mockAction3.coolOffPeriodInMillis());
            Assert.assertEquals(action.isMuted(), mockAction3.isMuted());
            Assert.assertEquals(action.getSummary(), mockAction3.summary());
            index++;
        }

    }

    public class MockAction implements Action {
        private String name;
        private List<String> nodeIps;

        public MockAction(String name, List<String> nodeIps) {
            this.name = name;
            this.nodeIps = nodeIps;
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
            String IP1 = (nodeIps.get(0) + ".").repeat(4);
            String IP2 = (nodeIps.get(1) + ".").repeat(4);
            nodeKeys.add(new NodeKey(new InstanceDetails.Id(nodeIps.get(0)), new InstanceDetails.Ip(IP1.substring(0, IP1.length() - 1))));
            nodeKeys.add(new NodeKey(new InstanceDetails.Id(nodeIps.get(1)), new InstanceDetails.Ip(IP2.substring(0, IP2.length() - 1))));
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
