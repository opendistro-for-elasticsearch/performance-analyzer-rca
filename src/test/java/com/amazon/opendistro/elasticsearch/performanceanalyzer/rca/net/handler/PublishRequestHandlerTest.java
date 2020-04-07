package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.net.handler;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.FlowUnitMessage;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.PublishResponse;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.net.NodeStateManager;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.net.ReceivedFlowUnitStore;
import io.grpc.stub.StreamObserver;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

public class PublishRequestHandlerTest {
    private PublishRequestHandler uut;

    @Mock
    private NodeStateManager nodeStateManager;

    @Mock
    private ReceivedFlowUnitStore receivedFlowUnitStore;

    @Mock
    private ExecutorService executorService;

    @Mock
    private StreamObserver<PublishResponse> serviceResponse;

    @Mock
    private StreamObserver<PublishResponse> serviceResponse2;

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
        AtomicReference<ExecutorService> reference = new AtomicReference<>(executorService);
        uut = new PublishRequestHandler(nodeStateManager, receivedFlowUnitStore, reference);
    }

    @Test
    public void testGetClientStream() {
        // upstream response stream list should initially be empty
        Assert.assertEquals(0, uut.getUpstreamResponseStreamList().size());
        // method call should add to the upstream response stream list
        StreamObserver<FlowUnitMessage> ret = uut.getClientStream(serviceResponse);
        Assert.assertEquals(1, uut.getUpstreamResponseStreamList().size());
        Assert.assertEquals(serviceResponse, uut.getUpstreamResponseStreamList().get(0));
        Assert.assertTrue(ret instanceof PublishRequestHandler.SendDataClientStreamUpdateConsumer);
        Assert.assertEquals(serviceResponse,
                ((PublishRequestHandler.SendDataClientStreamUpdateConsumer) ret).getServiceResponse());
        // method should handle multiple streams
        uut.getClientStream(serviceResponse2);
        Assert.assertEquals(2, uut.getUpstreamResponseStreamList().size());
        Assert.assertEquals(serviceResponse2, uut.getUpstreamResponseStreamList().get(1));
    }

    @Test
    public void testTerminateUpstreamConnections() {
        // Add upstream connections, then terminate
        uut.getClientStream(serviceResponse);
        uut.getClientStream(serviceResponse2);
        uut.terminateUpstreamConnections();
        // Verify that each connection was appropriately terminated
        ArgumentCaptor<PublishResponse> captor = ArgumentCaptor.forClass(PublishResponse.class);
        Mockito.verify(serviceResponse, Mockito.times(1)).onNext(captor.capture());
        Mockito.verify(serviceResponse, Mockito.times(1)).onCompleted();
        Assert.assertEquals(
                PublishResponse.PublishResponseStatus.NODE_SHUTDOWN, captor.getValue().getDataStatus());
        Mockito.verify(serviceResponse2, Mockito.times(1)).onNext(captor.capture());
        Mockito.verify(serviceResponse2, Mockito.times(1)).onCompleted();
        Assert.assertEquals(
                PublishResponse.PublishResponseStatus.NODE_SHUTDOWN, captor.getValue().getDataStatus());
    }
}
