package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.net.handler;

import static org.mockito.ArgumentMatchers.any;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.SubscribeMessage;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.SubscribeResponse;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.net.CompositeSubscribeRequest;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.net.SubscriptionManager;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.net.tasks.SubscriptionRxTask;
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

public class SubscribeServerHandlerTest {
    private SubscribeServerHandler uut;
    private AtomicReference<ExecutorService> reference = new AtomicReference<>();

    @Mock
    private SubscriptionManager subscriptionManager;

    @Mock
    private ExecutorService executorService;

    @Mock
    StreamObserver<SubscribeResponse> observer;

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
        reference.set(executorService);
        uut = new SubscribeServerHandler(subscriptionManager, reference);
    }

    @Test
    public void testHandleSubscriptionRequest() {
        // method shouldn't throw when executorService is null
        reference.set(null);
        SubscribeMessage request = SubscribeMessage.getDefaultInstance();
        uut.handleSubscriptionRequest(request, observer);
        // method should execute the expected SubscriptionRxTask when asked to handle a request
        reference.set(executorService);
        uut.handleSubscriptionRequest(request, observer);
        ArgumentCaptor<SubscriptionRxTask> captor = ArgumentCaptor.forClass(SubscriptionRxTask.class);
        Mockito.verify(executorService, Mockito.times(1)).execute(captor.capture());
        SubscriptionRxTask expected =
                new SubscriptionRxTask(subscriptionManager, new CompositeSubscribeRequest(request, observer));
        Assert.assertEquals(expected, captor.getValue());
        // method should survive a RejectedExecutionException
        Mockito.doThrow(new RejectedExecutionException()).when(executorService).execute(any());
        uut.handleSubscriptionRequest(request, observer); // shouldn't throw
    }
}
