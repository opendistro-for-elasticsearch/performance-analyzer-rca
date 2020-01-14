package com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc;

import static io.grpc.MethodDescriptor.generateFullMethodName;
import static io.grpc.stub.ClientCalls.asyncBidiStreamingCall;
import static io.grpc.stub.ClientCalls.asyncClientStreamingCall;
import static io.grpc.stub.ClientCalls.asyncServerStreamingCall;
import static io.grpc.stub.ClientCalls.asyncUnaryCall;
import static io.grpc.stub.ClientCalls.blockingServerStreamingCall;
import static io.grpc.stub.ClientCalls.blockingUnaryCall;
import static io.grpc.stub.ClientCalls.futureUnaryCall;
import static io.grpc.stub.ServerCalls.asyncBidiStreamingCall;
import static io.grpc.stub.ServerCalls.asyncClientStreamingCall;
import static io.grpc.stub.ServerCalls.asyncServerStreamingCall;
import static io.grpc.stub.ServerCalls.asyncUnaryCall;
import static io.grpc.stub.ServerCalls.asyncUnimplementedStreamingCall;
import static io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall;

/**
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 1.25.0)",
    comments = "Source: inter_node_rpc_service.proto")
public final class InterNodeRpcServiceGrpc {

  private InterNodeRpcServiceGrpc() {}

  public static final String SERVICE_NAME = "com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.InterNodeRpcService";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.FlowUnitMessage,
      com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.PublishResponse> getPublishMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "Publish",
      requestType = com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.FlowUnitMessage.class,
      responseType = com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.PublishResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.CLIENT_STREAMING)
  public static io.grpc.MethodDescriptor<com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.FlowUnitMessage,
      com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.PublishResponse> getPublishMethod() {
    io.grpc.MethodDescriptor<com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.FlowUnitMessage, com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.PublishResponse> getPublishMethod;
    if ((getPublishMethod = InterNodeRpcServiceGrpc.getPublishMethod) == null) {
      synchronized (InterNodeRpcServiceGrpc.class) {
        if ((getPublishMethod = InterNodeRpcServiceGrpc.getPublishMethod) == null) {
          InterNodeRpcServiceGrpc.getPublishMethod = getPublishMethod =
              io.grpc.MethodDescriptor.<com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.FlowUnitMessage, com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.PublishResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.CLIENT_STREAMING)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "Publish"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.FlowUnitMessage.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.PublishResponse.getDefaultInstance()))
              .setSchemaDescriptor(new InterNodeRpcServiceMethodDescriptorSupplier("Publish"))
              .build();
        }
      }
    }
    return getPublishMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.SubscribeMessage,
      com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.SubscribeResponse> getSubscribeMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "Subscribe",
      requestType = com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.SubscribeMessage.class,
      responseType = com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.SubscribeResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.SubscribeMessage,
      com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.SubscribeResponse> getSubscribeMethod() {
    io.grpc.MethodDescriptor<com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.SubscribeMessage, com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.SubscribeResponse> getSubscribeMethod;
    if ((getSubscribeMethod = InterNodeRpcServiceGrpc.getSubscribeMethod) == null) {
      synchronized (InterNodeRpcServiceGrpc.class) {
        if ((getSubscribeMethod = InterNodeRpcServiceGrpc.getSubscribeMethod) == null) {
          InterNodeRpcServiceGrpc.getSubscribeMethod = getSubscribeMethod =
              io.grpc.MethodDescriptor.<com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.SubscribeMessage, com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.SubscribeResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "Subscribe"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.SubscribeMessage.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.SubscribeResponse.getDefaultInstance()))
              .setSchemaDescriptor(new InterNodeRpcServiceMethodDescriptorSupplier("Subscribe"))
              .build();
        }
      }
    }
    return getSubscribeMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.MetricsRequest,
      com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.MetricsResponse> getGetMetricsMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GetMetrics",
      requestType = com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.MetricsRequest.class,
      responseType = com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.MetricsResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.MetricsRequest,
      com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.MetricsResponse> getGetMetricsMethod() {
    io.grpc.MethodDescriptor<com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.MetricsRequest, com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.MetricsResponse> getGetMetricsMethod;
    if ((getGetMetricsMethod = InterNodeRpcServiceGrpc.getGetMetricsMethod) == null) {
      synchronized (InterNodeRpcServiceGrpc.class) {
        if ((getGetMetricsMethod = InterNodeRpcServiceGrpc.getGetMetricsMethod) == null) {
          InterNodeRpcServiceGrpc.getGetMetricsMethod = getGetMetricsMethod =
              io.grpc.MethodDescriptor.<com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.MetricsRequest, com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.MetricsResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "GetMetrics"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.MetricsRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.MetricsResponse.getDefaultInstance()))
              .setSchemaDescriptor(new InterNodeRpcServiceMethodDescriptorSupplier("GetMetrics"))
              .build();
        }
      }
    }
    return getGetMetricsMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static InterNodeRpcServiceStub newStub(io.grpc.Channel channel) {
    return new InterNodeRpcServiceStub(channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static InterNodeRpcServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    return new InterNodeRpcServiceBlockingStub(channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static InterNodeRpcServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    return new InterNodeRpcServiceFutureStub(channel);
  }

  /**
   */
  public static abstract class InterNodeRpcServiceImplBase implements io.grpc.BindableService {

    /**
     * <pre>
     * Sends a flowunit to whoever is interested in it.
     * </pre>
     */
    public io.grpc.stub.StreamObserver<com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.FlowUnitMessage> publish(
        io.grpc.stub.StreamObserver<com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.PublishResponse> responseObserver) {
      return asyncUnimplementedStreamingCall(getPublishMethod(), responseObserver);
    }

    /**
     * <pre>
     * Sends a subscription request to a node for a particular metric.
     * </pre>
     */
    public void subscribe(com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.SubscribeMessage request,
        io.grpc.stub.StreamObserver<com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.SubscribeResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getSubscribeMethod(), responseObserver);
    }

    /**
     * <pre>
     * get Metrics for a particular node
     * </pre>
     */
    public void getMetrics(com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.MetricsRequest request,
        io.grpc.stub.StreamObserver<com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.MetricsResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getGetMetricsMethod(), responseObserver);
    }

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
          .addMethod(
            getPublishMethod(),
            asyncClientStreamingCall(
              new MethodHandlers<
                com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.FlowUnitMessage,
                com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.PublishResponse>(
                  this, METHODID_PUBLISH)))
          .addMethod(
            getSubscribeMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.SubscribeMessage,
                com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.SubscribeResponse>(
                  this, METHODID_SUBSCRIBE)))
          .addMethod(
            getGetMetricsMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.MetricsRequest,
                com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.MetricsResponse>(
                  this, METHODID_GET_METRICS)))
          .build();
    }
  }

  /**
   */
  public static final class InterNodeRpcServiceStub extends io.grpc.stub.AbstractStub<InterNodeRpcServiceStub> {
    private InterNodeRpcServiceStub(io.grpc.Channel channel) {
      super(channel);
    }

    private InterNodeRpcServiceStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected InterNodeRpcServiceStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new InterNodeRpcServiceStub(channel, callOptions);
    }

    /**
     * <pre>
     * Sends a flowunit to whoever is interested in it.
     * </pre>
     */
    public io.grpc.stub.StreamObserver<com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.FlowUnitMessage> publish(
        io.grpc.stub.StreamObserver<com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.PublishResponse> responseObserver) {
      return asyncClientStreamingCall(
          getChannel().newCall(getPublishMethod(), getCallOptions()), responseObserver);
    }

    /**
     * <pre>
     * Sends a subscription request to a node for a particular metric.
     * </pre>
     */
    public void subscribe(com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.SubscribeMessage request,
        io.grpc.stub.StreamObserver<com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.SubscribeResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getSubscribeMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * get Metrics for a particular node
     * </pre>
     */
    public void getMetrics(com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.MetricsRequest request,
        io.grpc.stub.StreamObserver<com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.MetricsResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getGetMetricsMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   */
  public static final class InterNodeRpcServiceBlockingStub extends io.grpc.stub.AbstractStub<InterNodeRpcServiceBlockingStub> {
    private InterNodeRpcServiceBlockingStub(io.grpc.Channel channel) {
      super(channel);
    }

    private InterNodeRpcServiceBlockingStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected InterNodeRpcServiceBlockingStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new InterNodeRpcServiceBlockingStub(channel, callOptions);
    }

    /**
     * <pre>
     * Sends a subscription request to a node for a particular metric.
     * </pre>
     */
    public com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.SubscribeResponse subscribe(com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.SubscribeMessage request) {
      return blockingUnaryCall(
          getChannel(), getSubscribeMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * get Metrics for a particular node
     * </pre>
     */
    public com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.MetricsResponse getMetrics(com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.MetricsRequest request) {
      return blockingUnaryCall(
          getChannel(), getGetMetricsMethod(), getCallOptions(), request);
    }
  }

  /**
   */
  public static final class InterNodeRpcServiceFutureStub extends io.grpc.stub.AbstractStub<InterNodeRpcServiceFutureStub> {
    private InterNodeRpcServiceFutureStub(io.grpc.Channel channel) {
      super(channel);
    }

    private InterNodeRpcServiceFutureStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected InterNodeRpcServiceFutureStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new InterNodeRpcServiceFutureStub(channel, callOptions);
    }

    /**
     * <pre>
     * Sends a subscription request to a node for a particular metric.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.SubscribeResponse> subscribe(
        com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.SubscribeMessage request) {
      return futureUnaryCall(
          getChannel().newCall(getSubscribeMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * get Metrics for a particular node
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.MetricsResponse> getMetrics(
        com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.MetricsRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getGetMetricsMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_SUBSCRIBE = 0;
  private static final int METHODID_GET_METRICS = 1;
  private static final int METHODID_PUBLISH = 2;

  private static final class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final InterNodeRpcServiceImplBase serviceImpl;
    private final int methodId;

    MethodHandlers(InterNodeRpcServiceImplBase serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_SUBSCRIBE:
          serviceImpl.subscribe((com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.SubscribeMessage) request,
              (io.grpc.stub.StreamObserver<com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.SubscribeResponse>) responseObserver);
          break;
        case METHODID_GET_METRICS:
          serviceImpl.getMetrics((com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.MetricsRequest) request,
              (io.grpc.stub.StreamObserver<com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.MetricsResponse>) responseObserver);
          break;
        default:
          throw new AssertionError();
      }
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public io.grpc.stub.StreamObserver<Req> invoke(
        io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_PUBLISH:
          return (io.grpc.stub.StreamObserver<Req>) serviceImpl.publish(
              (io.grpc.stub.StreamObserver<com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.PublishResponse>) responseObserver);
        default:
          throw new AssertionError();
      }
    }
  }

  private static abstract class InterNodeRpcServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    InterNodeRpcServiceBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.PANetworking.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("InterNodeRpcService");
    }
  }

  private static final class InterNodeRpcServiceFileDescriptorSupplier
      extends InterNodeRpcServiceBaseDescriptorSupplier {
    InterNodeRpcServiceFileDescriptorSupplier() {}
  }

  private static final class InterNodeRpcServiceMethodDescriptorSupplier
      extends InterNodeRpcServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final String methodName;

    InterNodeRpcServiceMethodDescriptorSupplier(String methodName) {
      this.methodName = methodName;
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.MethodDescriptor getMethodDescriptor() {
      return getServiceDescriptor().findMethodByName(methodName);
    }
  }

  private static volatile io.grpc.ServiceDescriptor serviceDescriptor;

  public static io.grpc.ServiceDescriptor getServiceDescriptor() {
    io.grpc.ServiceDescriptor result = serviceDescriptor;
    if (result == null) {
      synchronized (InterNodeRpcServiceGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new InterNodeRpcServiceFileDescriptorSupplier())
              .addMethod(getPublishMethod())
              .addMethod(getSubscribeMethod())
              .addMethod(getGetMetricsMethod())
              .build();
        }
      }
    }
    return result;
  }
}
