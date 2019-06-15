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
import java.util.concurrent.atomic.AtomicInteger;

import com.axiom.pipeline.core.FeatureRow;
import com.axiom.pipeline.util.Tuple;
import com.axiom.pipeline.core.StatefulAggregationDoFn;
import com.axiom.pipeline.core.FilterDuplicates;

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
    public PCollection<TableRow> expand(PCollection<Row> events){

        return events
            .apply("FilterRowsByExchange", ParDo.of(new DoFn<Row, Row>() {
                @ProcessElement
                public void processElement(ProcessContext c) {
                    if(c.element().getString("exchange").equals(exchange)){
                        c.output(c.element());
                    }
                }
            }))
            .apply("FilterRowsBySymbol", ParDo.of(new DoFn<Row, Row>() {
                @ProcessElement
                public void processElement(ProcessContext c) {
                    if(c.element().getString("symbol").equals(symbol)){
                        c.output(c.element());
                    }
                }
            }))
            .apply(
                "Add Row Timestamps",
                WithTimestamps.of(
                    (Row row) -> new Instant(row.getValue("time"))
                )
            )
            .apply(
                Window.<Row>into(
                    FixedWindows.of(interval)
                )
            )
            .apply("MapPartitionIdKey", ParDo.of(new DoFn<Row, KV<String, Row>>() {
                @ProcessElement
                public void processElement(ProcessContext c) {
                    c.output(KV.of(c.element().getValue("partition_id"), c.element()));
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
