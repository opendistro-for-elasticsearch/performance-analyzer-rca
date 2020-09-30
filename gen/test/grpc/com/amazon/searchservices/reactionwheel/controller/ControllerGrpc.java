package com.amazon.searchservices.reactionwheel.controller;

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
 * <pre>
 * A Controller accepts requests to control one or more Targets in a cluster. It also provides a method
 * for describing the progress of control requests.
 * </pre>
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 1.25.0)",
    comments = "Source: reaction_wheel.proto")
public final class ControllerGrpc {

  private ControllerGrpc() {}

  public static final String SERVICE_NAME = "controller.Controller";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<com.amazon.searchservices.reactionwheel.controller.ReactionWheel.BatchStartControlRequest,
      com.amazon.searchservices.reactionwheel.controller.ReactionWheel.BatchStartControlResult> getBatchStartControlMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "BatchStartControl",
      requestType = com.amazon.searchservices.reactionwheel.controller.ReactionWheel.BatchStartControlRequest.class,
      responseType = com.amazon.searchservices.reactionwheel.controller.ReactionWheel.BatchStartControlResult.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.amazon.searchservices.reactionwheel.controller.ReactionWheel.BatchStartControlRequest,
      com.amazon.searchservices.reactionwheel.controller.ReactionWheel.BatchStartControlResult> getBatchStartControlMethod() {
    io.grpc.MethodDescriptor<com.amazon.searchservices.reactionwheel.controller.ReactionWheel.BatchStartControlRequest, com.amazon.searchservices.reactionwheel.controller.ReactionWheel.BatchStartControlResult> getBatchStartControlMethod;
    if ((getBatchStartControlMethod = ControllerGrpc.getBatchStartControlMethod) == null) {
      synchronized (ControllerGrpc.class) {
        if ((getBatchStartControlMethod = ControllerGrpc.getBatchStartControlMethod) == null) {
          ControllerGrpc.getBatchStartControlMethod = getBatchStartControlMethod =
              io.grpc.MethodDescriptor.<com.amazon.searchservices.reactionwheel.controller.ReactionWheel.BatchStartControlRequest, com.amazon.searchservices.reactionwheel.controller.ReactionWheel.BatchStartControlResult>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "BatchStartControl"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.amazon.searchservices.reactionwheel.controller.ReactionWheel.BatchStartControlRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.amazon.searchservices.reactionwheel.controller.ReactionWheel.BatchStartControlResult.getDefaultInstance()))
              .setSchemaDescriptor(new ControllerMethodDescriptorSupplier("BatchStartControl"))
              .build();
        }
      }
    }
    return getBatchStartControlMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.amazon.searchservices.reactionwheel.controller.ReactionWheel.DescribeControlExecutionsRequest,
      com.amazon.searchservices.reactionwheel.controller.ReactionWheel.DescribeControlExecutionsResult> getDescribeControlExecutionsMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "DescribeControlExecutions",
      requestType = com.amazon.searchservices.reactionwheel.controller.ReactionWheel.DescribeControlExecutionsRequest.class,
      responseType = com.amazon.searchservices.reactionwheel.controller.ReactionWheel.DescribeControlExecutionsResult.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.amazon.searchservices.reactionwheel.controller.ReactionWheel.DescribeControlExecutionsRequest,
      com.amazon.searchservices.reactionwheel.controller.ReactionWheel.DescribeControlExecutionsResult> getDescribeControlExecutionsMethod() {
    io.grpc.MethodDescriptor<com.amazon.searchservices.reactionwheel.controller.ReactionWheel.DescribeControlExecutionsRequest, com.amazon.searchservices.reactionwheel.controller.ReactionWheel.DescribeControlExecutionsResult> getDescribeControlExecutionsMethod;
    if ((getDescribeControlExecutionsMethod = ControllerGrpc.getDescribeControlExecutionsMethod) == null) {
      synchronized (ControllerGrpc.class) {
        if ((getDescribeControlExecutionsMethod = ControllerGrpc.getDescribeControlExecutionsMethod) == null) {
          ControllerGrpc.getDescribeControlExecutionsMethod = getDescribeControlExecutionsMethod =
              io.grpc.MethodDescriptor.<com.amazon.searchservices.reactionwheel.controller.ReactionWheel.DescribeControlExecutionsRequest, com.amazon.searchservices.reactionwheel.controller.ReactionWheel.DescribeControlExecutionsResult>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "DescribeControlExecutions"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.amazon.searchservices.reactionwheel.controller.ReactionWheel.DescribeControlExecutionsRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.amazon.searchservices.reactionwheel.controller.ReactionWheel.DescribeControlExecutionsResult.getDefaultInstance()))
              .setSchemaDescriptor(new ControllerMethodDescriptorSupplier("DescribeControlExecutions"))
              .build();
        }
      }
    }
    return getDescribeControlExecutionsMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.amazon.searchservices.reactionwheel.controller.ReactionWheel.ListControlExecutionsRequest,
      com.amazon.searchservices.reactionwheel.controller.ReactionWheel.ListControlExecutionsResult> getListControlExecutionsMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "ListControlExecutions",
      requestType = com.amazon.searchservices.reactionwheel.controller.ReactionWheel.ListControlExecutionsRequest.class,
      responseType = com.amazon.searchservices.reactionwheel.controller.ReactionWheel.ListControlExecutionsResult.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.amazon.searchservices.reactionwheel.controller.ReactionWheel.ListControlExecutionsRequest,
      com.amazon.searchservices.reactionwheel.controller.ReactionWheel.ListControlExecutionsResult> getListControlExecutionsMethod() {
    io.grpc.MethodDescriptor<com.amazon.searchservices.reactionwheel.controller.ReactionWheel.ListControlExecutionsRequest, com.amazon.searchservices.reactionwheel.controller.ReactionWheel.ListControlExecutionsResult> getListControlExecutionsMethod;
    if ((getListControlExecutionsMethod = ControllerGrpc.getListControlExecutionsMethod) == null) {
      synchronized (ControllerGrpc.class) {
        if ((getListControlExecutionsMethod = ControllerGrpc.getListControlExecutionsMethod) == null) {
          ControllerGrpc.getListControlExecutionsMethod = getListControlExecutionsMethod =
              io.grpc.MethodDescriptor.<com.amazon.searchservices.reactionwheel.controller.ReactionWheel.ListControlExecutionsRequest, com.amazon.searchservices.reactionwheel.controller.ReactionWheel.ListControlExecutionsResult>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "ListControlExecutions"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.amazon.searchservices.reactionwheel.controller.ReactionWheel.ListControlExecutionsRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.amazon.searchservices.reactionwheel.controller.ReactionWheel.ListControlExecutionsResult.getDefaultInstance()))
              .setSchemaDescriptor(new ControllerMethodDescriptorSupplier("ListControlExecutions"))
              .build();
        }
      }
    }
    return getListControlExecutionsMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static ControllerStub newStub(io.grpc.Channel channel) {
    return new ControllerStub(channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static ControllerBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    return new ControllerBlockingStub(channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static ControllerFutureStub newFutureStub(
      io.grpc.Channel channel) {
    return new ControllerFutureStub(channel);
  }

  /**
   * <pre>
   * A Controller accepts requests to control one or more Targets in a cluster. It also provides a method
   * for describing the progress of control requests.
   * </pre>
   */
  public static abstract class ControllerImplBase implements io.grpc.BindableService {

    /**
     */
    public void batchStartControl(com.amazon.searchservices.reactionwheel.controller.ReactionWheel.BatchStartControlRequest request,
        io.grpc.stub.StreamObserver<com.amazon.searchservices.reactionwheel.controller.ReactionWheel.BatchStartControlResult> responseObserver) {
      asyncUnimplementedUnaryCall(getBatchStartControlMethod(), responseObserver);
    }

    /**
     */
    public void describeControlExecutions(com.amazon.searchservices.reactionwheel.controller.ReactionWheel.DescribeControlExecutionsRequest request,
        io.grpc.stub.StreamObserver<com.amazon.searchservices.reactionwheel.controller.ReactionWheel.DescribeControlExecutionsResult> responseObserver) {
      asyncUnimplementedUnaryCall(getDescribeControlExecutionsMethod(), responseObserver);
    }

    /**
     */
    public void listControlExecutions(com.amazon.searchservices.reactionwheel.controller.ReactionWheel.ListControlExecutionsRequest request,
        io.grpc.stub.StreamObserver<com.amazon.searchservices.reactionwheel.controller.ReactionWheel.ListControlExecutionsResult> responseObserver) {
      asyncUnimplementedUnaryCall(getListControlExecutionsMethod(), responseObserver);
    }

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
          .addMethod(
            getBatchStartControlMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                com.amazon.searchservices.reactionwheel.controller.ReactionWheel.BatchStartControlRequest,
                com.amazon.searchservices.reactionwheel.controller.ReactionWheel.BatchStartControlResult>(
                  this, METHODID_BATCH_START_CONTROL)))
          .addMethod(
            getDescribeControlExecutionsMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                com.amazon.searchservices.reactionwheel.controller.ReactionWheel.DescribeControlExecutionsRequest,
                com.amazon.searchservices.reactionwheel.controller.ReactionWheel.DescribeControlExecutionsResult>(
                  this, METHODID_DESCRIBE_CONTROL_EXECUTIONS)))
          .addMethod(
            getListControlExecutionsMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                com.amazon.searchservices.reactionwheel.controller.ReactionWheel.ListControlExecutionsRequest,
                com.amazon.searchservices.reactionwheel.controller.ReactionWheel.ListControlExecutionsResult>(
                  this, METHODID_LIST_CONTROL_EXECUTIONS)))
          .build();
    }
  }

  /**
   * <pre>
   * A Controller accepts requests to control one or more Targets in a cluster. It also provides a method
   * for describing the progress of control requests.
   * </pre>
   */
  public static final class ControllerStub extends io.grpc.stub.AbstractStub<ControllerStub> {
    private ControllerStub(io.grpc.Channel channel) {
      super(channel);
    }

    private ControllerStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected ControllerStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new ControllerStub(channel, callOptions);
    }

    /**
     */
    public void batchStartControl(com.amazon.searchservices.reactionwheel.controller.ReactionWheel.BatchStartControlRequest request,
        io.grpc.stub.StreamObserver<com.amazon.searchservices.reactionwheel.controller.ReactionWheel.BatchStartControlResult> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getBatchStartControlMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void describeControlExecutions(com.amazon.searchservices.reactionwheel.controller.ReactionWheel.DescribeControlExecutionsRequest request,
        io.grpc.stub.StreamObserver<com.amazon.searchservices.reactionwheel.controller.ReactionWheel.DescribeControlExecutionsResult> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getDescribeControlExecutionsMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void listControlExecutions(com.amazon.searchservices.reactionwheel.controller.ReactionWheel.ListControlExecutionsRequest request,
        io.grpc.stub.StreamObserver<com.amazon.searchservices.reactionwheel.controller.ReactionWheel.ListControlExecutionsResult> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getListControlExecutionsMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   * <pre>
   * A Controller accepts requests to control one or more Targets in a cluster. It also provides a method
   * for describing the progress of control requests.
   * </pre>
   */
  public static final class ControllerBlockingStub extends io.grpc.stub.AbstractStub<ControllerBlockingStub> {
    private ControllerBlockingStub(io.grpc.Channel channel) {
      super(channel);
    }

    private ControllerBlockingStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected ControllerBlockingStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new ControllerBlockingStub(channel, callOptions);
    }

    /**
     */
    public com.amazon.searchservices.reactionwheel.controller.ReactionWheel.BatchStartControlResult batchStartControl(com.amazon.searchservices.reactionwheel.controller.ReactionWheel.BatchStartControlRequest request) {
      return blockingUnaryCall(
          getChannel(), getBatchStartControlMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.amazon.searchservices.reactionwheel.controller.ReactionWheel.DescribeControlExecutionsResult describeControlExecutions(com.amazon.searchservices.reactionwheel.controller.ReactionWheel.DescribeControlExecutionsRequest request) {
      return blockingUnaryCall(
          getChannel(), getDescribeControlExecutionsMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.amazon.searchservices.reactionwheel.controller.ReactionWheel.ListControlExecutionsResult listControlExecutions(com.amazon.searchservices.reactionwheel.controller.ReactionWheel.ListControlExecutionsRequest request) {
      return blockingUnaryCall(
          getChannel(), getListControlExecutionsMethod(), getCallOptions(), request);
    }
  }

  /**
   * <pre>
   * A Controller accepts requests to control one or more Targets in a cluster. It also provides a method
   * for describing the progress of control requests.
   * </pre>
   */
  public static final class ControllerFutureStub extends io.grpc.stub.AbstractStub<ControllerFutureStub> {
    private ControllerFutureStub(io.grpc.Channel channel) {
      super(channel);
    }

    private ControllerFutureStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected ControllerFutureStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new ControllerFutureStub(channel, callOptions);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.amazon.searchservices.reactionwheel.controller.ReactionWheel.BatchStartControlResult> batchStartControl(
        com.amazon.searchservices.reactionwheel.controller.ReactionWheel.BatchStartControlRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getBatchStartControlMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.amazon.searchservices.reactionwheel.controller.ReactionWheel.DescribeControlExecutionsResult> describeControlExecutions(
        com.amazon.searchservices.reactionwheel.controller.ReactionWheel.DescribeControlExecutionsRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getDescribeControlExecutionsMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.amazon.searchservices.reactionwheel.controller.ReactionWheel.ListControlExecutionsResult> listControlExecutions(
        com.amazon.searchservices.reactionwheel.controller.ReactionWheel.ListControlExecutionsRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getListControlExecutionsMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_BATCH_START_CONTROL = 0;
  private static final int METHODID_DESCRIBE_CONTROL_EXECUTIONS = 1;
  private static final int METHODID_LIST_CONTROL_EXECUTIONS = 2;

  private static final class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final ControllerImplBase serviceImpl;
    private final int methodId;

    MethodHandlers(ControllerImplBase serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_BATCH_START_CONTROL:
          serviceImpl.batchStartControl((com.amazon.searchservices.reactionwheel.controller.ReactionWheel.BatchStartControlRequest) request,
              (io.grpc.stub.StreamObserver<com.amazon.searchservices.reactionwheel.controller.ReactionWheel.BatchStartControlResult>) responseObserver);
          break;
        case METHODID_DESCRIBE_CONTROL_EXECUTIONS:
          serviceImpl.describeControlExecutions((com.amazon.searchservices.reactionwheel.controller.ReactionWheel.DescribeControlExecutionsRequest) request,
              (io.grpc.stub.StreamObserver<com.amazon.searchservices.reactionwheel.controller.ReactionWheel.DescribeControlExecutionsResult>) responseObserver);
          break;
        case METHODID_LIST_CONTROL_EXECUTIONS:
          serviceImpl.listControlExecutions((com.amazon.searchservices.reactionwheel.controller.ReactionWheel.ListControlExecutionsRequest) request,
              (io.grpc.stub.StreamObserver<com.amazon.searchservices.reactionwheel.controller.ReactionWheel.ListControlExecutionsResult>) responseObserver);
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
        default:
          throw new AssertionError();
      }
    }
  }

  private static abstract class ControllerBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    ControllerBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return com.amazon.searchservices.reactionwheel.controller.ReactionWheel.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("Controller");
    }
  }

  private static final class ControllerFileDescriptorSupplier
      extends ControllerBaseDescriptorSupplier {
    ControllerFileDescriptorSupplier() {}
  }

  private static final class ControllerMethodDescriptorSupplier
      extends ControllerBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final String methodName;

    ControllerMethodDescriptorSupplier(String methodName) {
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
      synchronized (ControllerGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new ControllerFileDescriptorSupplier())
              .addMethod(getBatchStartControlMethod())
              .addMethod(getDescribeControlExecutionsMethod())
              .addMethod(getListControlExecutionsMethod())
              .build();
        }
      }
    }
    return result;
  }
}
