package com.axiom.pipeline.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.List;
import java.util.Map;

import com.axiom.pipeline.datum.Event;
import com.google.api.services.bigquery.model.TableRow;
import org.joda.time.Duration;
import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.state.BagState;
import org.apache.beam.sdk.state.TimerSpec;
import org.apache.beam.sdk.state.TimerSpecs;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.joda.time.Instant;
import org.apache.beam.sdk.values.Row;

import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.transforms.WithTimestamps;

public class StatefulCombine extends PTransform<PCollection<Event>, PCollection<TableRow>> {
    private static final Logger logger = LoggerFactory.getLogger(StatefulCombine.class);

    final String exchange;
    final String quote_asset;
    final String base_asset;
    final String symbol;
    final Duration interval;
    final int numLevels;

    public StatefulCombine(
        String exchange,
        String quote_asset,
        String base_asset,
        Duration interval,
        int numLevels
    ) {
        this.exchange=exchange;
        this.quote_asset=quote_asset;
        this.base_asset=base_asset;
        this.symbol=base_asset+quote_asset;
        this.interval=interval;
        this.numLevels=numLevels;
    }

    @Override
    public PCollection<TableRow> expand(PCollection<Event> events){

        return events
            .apply(
                "Add Event Timestamps",
                WithTimestamps.of(
                    (Event event) -> new Instant(event.getTime())
                )
            )
            .apply(
                Window.<Event>into(
                    FixedWindows.of(interval)
                )
            )
            .apply("MapRowPartitionIdKey", ParDo.of(new DoFn<Event, KV<String, Row>>() {
                @ProcessElement
                public void processElement(ProcessContext c) {
                    c.output(KV.of(c.element().getPartitionString(), c.element().toRow()));
                }
            }))
            .apply("StatefulAggregation", ParDo.of(new StatefulAggregationDoFn(
                exchange,
                quote_asset,
                base_asset,
                interval,
                numLevels
            )));

    }



}
