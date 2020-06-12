package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.grpc;

import io.grpc.Channel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

class GRPCClient {
    private static final Logger LOG = LogManager.getLogger(GRPCClient.class);
    private Channel channel;
    private InterNodeRpcServiceGrpc.InterNodeRpcServiceBlockingStub client;
    private InterNodeRpcServiceGrpc.InterNodeRpcServiceStub asyncClient;

    public GRPCClient(String host, int port) {
        this(ManagedChannelBuilder.forAddress(host, port).usePlaintext());
    }

    public GRPCClient(ManagedChannelBuilder<?> channelBuilder) {
        channel = channelBuilder.build();
        client = InterNodeRpcServiceGrpc.newBlockingStub(channel);
        asyncClient = InterNodeRpcServiceGrpc.newStub(channel);
    }

    // Get Metrics https://opendistro.github.io/for-elasticsearch-docs/docs/pa/reference/
    public MetricsResponse getMetrics(MetricsRequest request) {
        return client.getMetrics(request);
    }

    // Request to subscribe to messages from an upstream node
    public SubscribeResponse subscribe(SubscribeMessage request) {
        return client.subscribe(request);
    }

    // Publish a series of FlowUnitMessages
    public PublishResponse publish(List<FlowUnitMessage> messages) throws InterruptedException {
        PublishResponseObserver publishObserver = new PublishResponseObserver();
        StreamObserver<FlowUnitMessage> responseObserver = asyncClient.publish(publishObserver);
        for (FlowUnitMessage message : messages) {
            responseObserver.onNext(message);
        }
        responseObserver.onCompleted();
        publishObserver.finishLatch.await(1, TimeUnit.MINUTES);
        return publishObserver.getResponse();
    }

    private class PublishResponseObserver implements StreamObserver<PublishResponse> {
        private PublishResponse response;
        final CountDownLatch finishLatch = new CountDownLatch(1);

        public PublishResponse getResponse() {
            return response;
        }

        @Override
        public void onNext(PublishResponse value) {
            this.response = value;
            LOG.info("Received PublishResponse: {}", value);
        }

        @Override
        public void onError(Throwable t) {
            LOG.error("Encountered error during subscribe calL.", t);
            finishLatch.countDown();
        }

        @Override
        public void onCompleted() {
            LOG.info("subscribe request completed successfully");
            finishLatch.countDown();
        }
    }
}
