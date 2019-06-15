package com.axiom.pipeline.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.List;
import java.util.Map;

import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.coders.BigEndianLongCoder;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollectionTuple;

import org.apache.beam.sdk.transforms.windowing.Window;
import org.joda.time.Instant;
import org.apache.beam.sdk.transforms.WithTimestamps;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.joda.time.Duration;
import org.apache.beam.sdk.transforms.ParDo;
import com.axiom.pipeline.util.Json.JsonException;
import com.axiom.pipeline.util.Json;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.values.PCollectionList;

import com.google.common.collect.Lists;
import org.apache.beam.sdk.schemas.transforms.Group;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.state.BagState;
import org.apache.beam.sdk.state.TimerSpec;
import org.apache.beam.sdk.state.TimerSpecs;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.state.Timer;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import java.util.HashMap;
import org.apache.beam.sdk.io.TextIO;
import com.google.common.collect.ImmutableList;
import java.util.Comparator;
import java.util.Collections;

import com.google.api.services.bigquery.model.TableSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableFieldSchema;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import com.google.api.services.bigquery.model.TableReference;

import com.axiom.pipeline.datum.AvroPubsubMessage;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.state.MapState;
import org.apache.beam.sdk.state.CombiningState;
import java.util.stream.Stream;
import java.util.stream.Collectors;
import org.apache.beam.sdk.options.ValueProvider;
import java.util.DoubleSummaryStatistics;
import java.util.stream.Collector;
import org.apache.beam.sdk.coders.MapCoder;
import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.apache.beam.sdk.coders.DoubleCoder;
import java.util.ArrayList;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import com.axiom.pipeline.core.FeatureRow;
import com.axiom.pipeline.util.Tuple;
import com.axiom.pipeline.core.SideCombineFn;
import com.axiom.pipeline.util.Compare;

public class StatefulAggregationDoFn extends DoFn<KV<String, Row>, TableRow>{
    private static final Logger logger = LoggerFactory.getLogger(StatefulAggregationDoFn.class);

    final String exchange;
    final String quote_asset;
    final String base_asset;
    final Duration interval;
    final int numLevels;

    private final Schema levelSchema = Schema.builder()
        .addInt32Field("level")
        .addDoubleField("price")
        .addDoubleField("quantity")
        .addInt64Field("time")
        .addStringField("side")
        .build();

    public StatefulAggregationDoFn(
        String exchange,
        String quote_asset,
        String base_asset,
        Duration interval,
        int numLevels
    ) {
        this.exchange=exchange;
        this.quote_asset=quote_asset;
        this.base_asset=base_asset;
        this.interval=interval;
        this.numLevels=numLevels;
    }

    @StateId("trade_buffer")
    private final StateSpec<BagState<Row>> bufferedTrades = StateSpecs.bag();

    @StateId("depth_buffer")
    private final StateSpec<BagState<Row>> bufferedDepths = StateSpecs.bag();

    @StateId("bids")
    private final StateSpec<CombiningState<Row, Map<Double, Double>, Map<Double, Double>>> bidStateSpec = StateSpecs.combining(
        MapCoder.of(DoubleCoder.of(), DoubleCoder.of()),
        new SideCombineFn(
            10,
            "bid"
        )
    );

    @StateId("asks")
    private final StateSpec<CombiningState<Row, Map<Double, Double>, Map<Double, Double>>> askStateSpec = StateSpecs.combining(
        MapCoder.of(DoubleCoder.of(), DoubleCoder.of()),
        new SideCombineFn(
            10,
            "ask"
        )
    );

    @TimerId("expiry")
    private final TimerSpec expirySpec = TimerSpecs.timer(TimeDomain.EVENT_TIME);

    @StateId("windowEnd")
    private final StateSpec<ValueState<Long>> windowEndState = StateSpecs.value();
                
    @ProcessElement
    public void process(
        ProcessContext context,
        BoundedWindow window,
        @StateId("trade_buffer") BagState<Row> depthBufferState,
        @StateId("depth_buffer") BagState<Row> tradeBufferState,
        @StateId("windowEnd") ValueState<Long> windowEndState,
        @TimerId("expiry") Timer expiryTimer
    ) {
        windowEndState.write(Long.valueOf(window.maxTimestamp().getMillis()));
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
        OnTimerContext context,                    
        @StateId("trade_buffer") BagState<Row> depthBufferState,
        @StateId("depth_buffer") BagState<Row> tradeBufferState,
        @StateId("windowEnd") ValueState<Long> windowEndState,
        @StateId("asks") CombiningState<Row, Map<Double, Double>, Map<Double, Double>> askState,
        @StateId("bids") CombiningState<Row, Map<Double, Double>, Map<Double, Double>> bidState
    ) {

        FeatureRow row = new FeatureRow();

        if (!depthBufferState.isEmpty().read()) {
            List<Row> depths = Lists.newArrayList(depthBufferState.read());
            List<Row> sortedDepths = depths.stream().sorted(new Compare.TimeComparator()).collect(Collectors.toList());

            List<Row> depthUpdates = new ArrayList<Row>();

            for (Row levelUpdate : sortedDepths) { // TODO sort by time
                
                if (levelUpdate.getString("side").equals("bid")) {
                    bidState.add(levelUpdate);
                } else if (levelUpdate.getString("side").equals("ask")) {
                    askState.add(levelUpdate);
                } else {
                    logger.error("Side not allowed");
                }
                
                depthUpdates.addAll(enrichLevelUpdate(
                    levelUpdate,
                    this.numLevels,
                    "ask",
                    askState.read().entrySet()
                ));

                depthUpdates.addAll(enrichLevelUpdate(
                    levelUpdate,
                    this.numLevels,
                    "bid",
                    bidState.read().entrySet()
                ));
            }

            depthUpdates.stream()
                .collect(
                    Collectors.groupingBy(r -> 
                        new Tuple(
                            r.getValue("side"), 
                            r.getValue("level")
                        )
                    )
                )
                .entrySet()
                .stream()
                .forEach(e -> row.aggregate(e.getValue(), e.getKey().toLabel()));
        }

        if (!tradeBufferState.isEmpty().read()) {
            List<Row> trades = Lists.newArrayList(tradeBufferState.read());
            row.aggregate(trades, "all");

            List<Row> buy_trades = trades.stream()
                .filter(o -> o.getString("side").equals("buy"))
                .collect(Collectors.toList()); 
            row.aggregate(trades, "buy");

            List<Row> sell_trades = trades.stream()
                .filter(o -> o.getString("side").equals("sell"))
                .collect(Collectors.toList()); 
            row.aggregate(trades, "sell");
        }

        TableRow outputTableRow = row.getTableRow();

        outputTableRow
            .set("exchange", exchange)
            .set("quote_asset", quote_asset)
            .set("base_asset", base_asset)
            .set("interval", interval.getStandardSeconds())
            .set("window_end", windowEndState.read());

        // System.out.println(outputTableRow.toString());

        context.output(outputTableRow);

        depthBufferState.clear();
        tradeBufferState.clear();
    }

    public List<Row> enrichLevelUpdate(Row levelUpdate, int levelNum, String side, Set<Entry<Double, Double>> state) {
        AtomicInteger index = new AtomicInteger();
        return state.stream().sorted(new Compare.LevelComparator(side))
        .limit(levelNum)
        .map(e -> Row.withSchema(levelSchema)
        .addValues(
            index.getAndIncrement(),
            e.getKey(),
            e.getValue(),
            levelUpdate.getInt64("time"),
            side
        ).build())
        .collect(Collectors.toList());
    }
}