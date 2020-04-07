package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.net.handler;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.FlowUnitMessage;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.PublishResponse;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.net.NodeStateManager;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.net.ReceivedFlowUnitStore;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.net.tasks.FlowUnitRxTask;
import io.grpc.stub.StreamObserver;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

public class SendDataClientStreamUpdateConsumerTest {
    private PublishRequestHandler.SendDataClientStreamUpdateConsumer uut;
    private AtomicReference<ExecutorService> reference;


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
        reference = new AtomicReference<>(executorService);
        PublishRequestHandler publishRequestHandler = new PublishRequestHandler(nodeStateManager, receivedFlowUnitStore, reference);
        uut = ((PublishRequestHandler.SendDataClientStreamUpdateConsumer)
                publishRequestHandler.getClientStream(serviceResponse));
    }

    @Test
    public void testOnNext() {
        // method shouldn't throw when executorService is null
        reference.set(null);
        uut.onNext(FlowUnitMessage.newBuilder().build());
        // method should execute the expected FlowUnitRxTask when asked to handle a request
        reference.set(executorService);
        FlowUnitMessage msg = FlowUnitMessage.newBuilder().build();
        ArgumentCaptor<Runnable> captor = ArgumentCaptor.forClass(Runnable.class);
        uut.onNext(msg);
        Mockito.verify(executorService, times(1)).execute(captor.capture());
        Assert.assertTrue(captor.getValue() instanceof FlowUnitRxTask);
        FlowUnitRxTask rx = (FlowUnitRxTask) captor.getValue();
        FlowUnitRxTask expected = new FlowUnitRxTask(nodeStateManager, receivedFlowUnitStore, msg);
        Assert.assertEquals(expected, rx);
        // method should survive a RejectedExecutionException
        Mockito.doThrow(new RejectedExecutionException()).when(executorService).execute(any());
        uut.onNext(msg);
    }

    @Test
    public void testOnError() {
        uut.onError(new Exception("exception")); // noop besides logging
    }

    @Test
    public void testOnCompleted() {
        ArgumentCaptor<PublishResponse> captor = ArgumentCaptor.forClass(PublishResponse.class);
        uut.onCompleted();
        Mockito.verify(serviceResponse, times(1)).onNext(captor.capture());
        Assert.assertEquals(PublishResponse.PublishResponseStatus.SUCCESS, captor.getValue().getDataStatus());
        Mockito.verify(serviceResponse, times(1)).onCompleted();

    }
}
