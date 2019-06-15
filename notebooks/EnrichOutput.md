package com.axiom.pipeline.core;


public class EnrichOutput extends Ptransform {

    public StatefulAggregationDoFn(
        Duration maxWait
    ) {
        this.maxWait=maxWait;
    }

    @StateId("output_buffer")
    private final StateSpec<BagState<Row>> bufferedTrades = StateSpecs.bag();

    @StateId("next_state_buffer")
    private final StateSpec<BagState<Row>> bufferedDepths = StateSpecs.bag();

    @ProcessElement
    public void process(
        ProcessContext context,
        @StateId("trade_buffer") BagState<Row> depthBufferState,
        @StateId("depth_buffer") BagState<Row> tradeBufferState,
        @StateId("windowEnd") ValueState<Long> windowEndState,
        @TimerId("expiry") Timer expiryTimer
    ) {
        expiryTimer.set(window.maxTimestamp());
        Row event = context.element().getValue();
        if (event.getValue("event_type").equals("trade")) {
            tradeBufferState.add(event);
        } else if (event.getValue("event_type").equals("levelUpdate")) {
            depthBufferState.add(event);                        
        } else {
            logger.error("Event type not supported");
        }
    }

    @OnTimer("expiry")
    public void onExpiry(

    }

}