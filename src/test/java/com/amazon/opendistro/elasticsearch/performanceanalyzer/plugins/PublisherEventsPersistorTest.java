package com.amazon.opendistro.elasticsearch.performanceanalyzer.plugins;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.Action;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.ImpactVector;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.ResourceEnum;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.util.InstanceDetails;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.persistence.Persistable;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.persistence.SQLitePersistor;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.persistence.actions.ActionsSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.cluster.NodeKey;
import java.io.IOException;
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

    private PublisherEventsPersistor publisherEventsPersistor;

    @Before
    public void init() throws IOException {
        String cwd = System.getProperty("user.dir");
        testLocation = Paths.get(cwd, "src", "test", "resources", "tmp", "file_rotate");
        Files.createDirectories(testLocation);
        FileUtils.cleanDirectory(testLocation.toFile());
        publisherEventsPersistor = new PublisherEventsPersistor();
    }

    @After
    public void cleanup() throws IOException {
        FileUtils.cleanDirectory(testLocation.toFile());
    }

    @Test
    public void actionPublished() throws Exception {
        final Persistable persistor = new SQLitePersistor(
                testLocation.toString(), baseFilename, String.valueOf(1), TimeUnit.SECONDS, 1);
        final MockAction mockAction = new MockAction();

        PublisherEventsPersistor.setPersistable(persistor);
        publisherEventsPersistor.actionPublished(mockAction);

        ActionsSummary actionsSummary = persistor.read(ActionsSummary.class);
        Assert.assertNotNull(actionsSummary);
        Assert.assertEquals(actionsSummary.getActionName(), mockAction.name());
        Assert.assertEquals(actionsSummary.getResourceValue(), mockAction.getResource().getNumber());
        Assert.assertEquals(
                actionsSummary.getId(), mockAction.impactedNodes().get(mockAction.impactedNodes().size() - 1).getNodeId().toString());
        Assert.assertEquals(
                actionsSummary.getIp(), mockAction.impactedNodes().get(mockAction.impactedNodes().size() - 1).getHostAddress().toString());
        Assert.assertEquals(actionsSummary.isActionable(), mockAction.isActionable());
        Assert.assertEquals(actionsSummary.getCoolOffPeriod(), mockAction.coolOffPeriodInMillis());
    }

    public class MockAction implements Action {

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
            return "MockAction";
        }

        @Override
        public String summary() {
            return null;
        }

        @Override
        public boolean isMuted() {
            return false;
        }

        @Override
        public ResourceEnum getResource() {
            return ResourceEnum.FIELD_DATA_CACHE;
        }
    }
}
