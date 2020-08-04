package com.amazon.opendistro.elasticsearch.performanceanalyzer.plugins.reaction_wheel;

import com.amazon.searchservices.reactionwheel.controller.ControllerGrpc;
import com.amazon.searchservices.reactionwheel.controller.ReactionWheel;
import com.amazon.searchservices.reactionwheel.controller.ReactionWheel.BatchStartControlRequest;
import com.amazon.searchservices.reactionwheel.controller.ReactionWheel.BatchStartControlResult;
import com.amazon.searchservices.reactionwheel.controller.ReactionWheel.ControlResult;
import com.amazon.searchservices.reactionwheel.controller.ReactionWheel.DescribeControlExecutionsRequest;
import com.amazon.searchservices.reactionwheel.controller.ReactionWheel.DescribeControlExecutionsResult;
import org.junit.Assert;

public class ReactionWheelDummyService extends ControllerGrpc.ControllerImplBase {
  public static final String CONTROL_ID = "CONTROL_ID_1";
  public static final int SERVER_PORT = 9100;
  private BatchStartControlResult batchResult;

  public ReactionWheelDummyService() {
    batchResult = null;
  }

  public BatchStartControlResult getAndClearResult() {
    BatchStartControlResult res = batchResult;
    batchResult = null;
    return res;
  }

  @Override
  public void batchStartControl(BatchStartControlRequest request,
      io.grpc.stub.StreamObserver<BatchStartControlResult> responseObserver) {

    //we are not supposed to send an empty request to RW
    Assert.assertTrue(request.getActionsCount() > 0);

    ReactionWheel.Action action = request.getActions(0);
    ReactionWheel.Control control = action.getControl();
    ReactionWheel.Target target = action.getTarget();

    ControlResult controlResult = ControlResult.newBuilder()
        .setTarget(target)
        .setControl(control)
        .setControlId(CONTROL_ID)
        .build();

    //Build the result
    BatchStartControlResult batchResult = BatchStartControlResult.newBuilder()
        .addResults(controlResult)
        .build();

    this.batchResult = batchResult;
    //Now tell the StreamObserver how to respond (i.e. with the batchResult)
    responseObserver.onNext(batchResult);
    responseObserver.onCompleted();
  }

  @Override
  public void describeControlExecutions(DescribeControlExecutionsRequest request,
      io.grpc.stub.StreamObserver<DescribeControlExecutionsResult> responseObserver) {
    //TODO: implement this function later as Decision Maker is not actively using this API
    Assert.fail();
  }
}
