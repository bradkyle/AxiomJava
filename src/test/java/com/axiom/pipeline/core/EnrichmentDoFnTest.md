
/*
 * Copyright (C) 2018 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */


package com.axiom.aggregator.core;

import com.axiom.pipeline.util.Json;
import com.axiom.pipeline.parsers.DepthParser;

import  org.hamcrest.Matchers;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import org.apache.beam.sdk.transforms.SerializableFunction;
import java.util.Arrays;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.joda.time.Instant;
import org.junit.Rule;
import java.net.URL;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;
import org.junit.rules.TemporaryFolder;
import org.junit.Ignore;
import java.io.File;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.avro.file.CodecFactory;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.options.ValueProvider;
import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.stream.Collectors;
import static java.util.stream.Collectors.toList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.beam.sdk.values.PCollectionTuple;
import com.fasterxml.jackson.core.type.TypeReference;
import java.util.Objects;
import java.nio.file.Files;
import java.nio.file.Paths;
import com.google.gson.GsonBuilder;
import org.json.JSONObject;
import org.json.JSONArray;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.Row;

import static com.axiom.pipeline.util.TestUtils.Valid;
import static com.axiom.pipeline.util.TestUtils.readFile;
import com.axiom.pipeline.datum.AvroPubsubMessage;
import com.axiom.pipeline.datum.Failure;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

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

import com.axiom.pipeline.parsers.TradeParser;
import com.axiom.pipeline.parsers.DepthParser;
import com.axiom.pipeline.datum.Failure;

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
import com.axiom.pipeline.core.FeatureSchema;
import java.util.stream.Stream;
import com.axiom.pipeline.core.StatefulCombine;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.Row;
import com.axiom.pipeline.datum.AvroPubsubMessage;
import java.util.Random;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import com.google.api.services.bigquery.model.TableRow;
import static com.axiom.pipeline.util.TestUtils.Valid;
import org.apache.beam.sdk.transforms.Count;

import org.apache.beam.sdk.coders.AvroCoder;
import com.axiom.pipeline.datum.Event;
import com.axiom.pipeline.core.EnrichmentDoFn;
import com.axiom.pipeline.core.StatefulAggregationDoFn;

public class EnrichmentDoFnTest {
    private static final Logger logger = LoggerFactory.getLogger(EnrichmentDoFnTest.class);
    private static final Duration ALLOWED_LATENESS = Duration.standardHours(1);
    private static final Duration WINDOW_DURATION = Duration.standardMinutes(2);
    private static final String EXCHANGE = "okex_spot";
    private static final String SYMBOL = "ETHBTC";
    private Instant baseTime = new Instant(0);
    @Rule public final transient TestPipeline TEST_PIPELINE = TestPipeline.create();
    private static final String ALPHA_NUMERIC_STRING = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";

    public static String randomAlphaNumeric(int count) {
        StringBuilder builder = new StringBuilder();
        while (count-- != 0) {
        int character = (int)(Math.random()*ALPHA_NUMERIC_STRING.length());
        builder.append(ALPHA_NUMERIC_STRING.charAt(character));
        }
        return builder.toString();
    }

    /** Some example users, on two separate teams. */
    private enum TestEvent {
        TRADE_ONE("trade", "okex_spot", "BTC", "ETH", 55.0, 44.0, "buy"),
        TRADE_TWO("trade", "okex_spot", "BTC", "ETH", 5.0, 4.0, "buy"),
        TRADE_THREE("trade", "okex_spot", "BTC", "ETH", 55.0, 44.0, "sell"),
        TRADE_FOUR("trade", "okex_spot", "BTC", "ETH", 55.0, 44.0, "sell"),
        BID_ONE("levelUpdate", "okex_spot", "BTC", "ETH", 5.0, 4.0, "bid"),
        BID_TWO("levelUpdate", "okex_spot", "BTC", "ETH", 6.0, 5.0, "bid"),
        BID_THREE("levelUpdate", "okex_spot", "BTC", "ETH", 7.0, 6.0, "bid"),
        BID_FOUR("levelUpdate", "okex_spot", "BTC", "ETH", 8.0, 7.0, "bid"),
        BID_FIVE("levelUpdate", "okex_spot", "BTC", "ETH", 0.0, 4.0, "bid"),
        BID_SIX("levelUpdate", "okex_spot", "BTC", "ETH", 1.0, 5.0, "bid"),
        ASK_ONE("levelUpdate", "okex_spot", "BTC", "ETH", 5.0, 4.0, "ask"),
        ASK_TWO("levelUpdate", "okex_spot", "BTC", "ETH", 6.0, 5.0, "ask"),
        ASK_THREE("levelUpdate", "okex_spot", "BTC", "ETH", 7.0, 6.0, "ask"),
        ASK_FOUR("levelUpdate", "okex_spot", "BTC", "ETH", 8.0, 7.0, "ask"),
        ASK_FIVE("levelUpdate", "okex_spot", "BTC", "ETH", 0.0, 4.0, "ask"),
        ASK_SIX("levelUpdate", "okex_spot", "BTC", "ETH", 1.0, 5.0, "ask");

        private final String eventType; 
        private final String exchange; 
        private final String quote_asset; 
        private final String base_asset; 
        private final Double quantity; 
        private final Double price; 
        private final String side; 

        TestEvent(
            String eventType, 
            String exchange, 
            String quote_asset, 
            String base_asset, 
            Double quantity, 
            Double price, 
            String side
        ) {
            Random rand = new Random();

            this.eventType=eventType; 
            this.exchange=exchange; 
            this.quote_asset=quote_asset; 
            this.base_asset=base_asset; 
            this.quantity=quantity; 
            this.price=price; 
            this.side=side; 
        }

        public String getEventType(){
            return this.eventType;
        }

        public String getExchange(){
            return this.exchange;
        }

        public String getQuote(){
            return this.quote_asset;
        }

        public String getBase(){
            return this.base_asset;
        }

        public Double getQuantity(){
            return this.quantity;
        }

        public Double getPrice(){
            return this.price;
        }

        public String getSide(){
            return this.side;
        }
    }


    @Test
    @Category(NeedsRunner.class)
    public void testStatefulCombine() throws Exception {

        TestStream<Event> createEvents =
            TestStream.create(AvroCoder.of(Event.class))
                // Start at the epoch
                .advanceWatermarkTo(baseTime)
                // add some elements ahead of the watermark
                .addElements(
                    event(TestEvent.TRADE_ONE, Duration.standardSeconds(3)),
                    event(TestEvent.TRADE_TWO, Duration.standardMinutes(1)),
                    event(TestEvent.BID_ONE, Duration.standardSeconds(22)),
                    event(TestEvent.BID_TWO, Duration.standardMinutes(1)),
                    event(TestEvent.BID_THREE, Duration.standardSeconds(22)),
                    event(TestEvent.BID_FOUR, Duration.standardMinutes(1)),
                    event(TestEvent.BID_FIVE, Duration.standardMinutes(1)),
                    event(TestEvent.ASK_ONE, Duration.standardSeconds(22)),
                    event(TestEvent.ASK_TWO, Duration.standardMinutes(1)),
                    event(TestEvent.ASK_THREE, Duration.standardSeconds(22)),
                    event(TestEvent.ASK_FOUR, Duration.standardMinutes(1)),
                    event(TestEvent.ASK_FIVE, Duration.standardMinutes(1))
                )
                // The watermark advances slightly, but not past the end of the window
                .advanceWatermarkTo(baseTime.plus(Duration.standardMinutes(3)))
                // Add some more on time elements
                .addElements(
                    event(TestEvent.BID_ONE, Duration.standardMinutes(4)),
                    event(TestEvent.TRADE_ONE, Duration.standardMinutes(4)),
                    event(TestEvent.TRADE_TWO, Duration.standardSeconds(270))
                )
                // The window should close and emit an ON_TIME pane
                .advanceWatermarkToInfinity();

        PCollection<TableRow> results = TEST_PIPELINE.apply(createEvents)
            .apply("MapRowPartitionIdKey", ParDo.of(new DoFn<Event, KV<String, Row>>() {
                @ProcessElement
                public void processElement(ProcessContext c) {
                    c.output(KV.of(c.element().getPartitionString(), c.element().toRow()));
                }
            }))
            .apply("StatefulAggregation", ParDo.of(new StatefulAggregationDoFn(
                "OKEX_SPOT",
                "QUOTE",
                "BASE",
                Duration.standardMinutes(5),
                5
            )));

   

        // PCollection<String> stresults = results.apply("ConvertToString", ParDo.of(new DoFn<TableRow, String>() {
        //     @ProcessElement
        //     public void processElement(ProcessContext c) {
        //         System.out.println(c.element().toString());
        //         c.output(c.element().toString());
        //     }
        // }));

        // Valid<TableRow> tableRowValidator = new Valid<TableRow>((datum) -> {
        //         assertNotNull(datum.get("symbol"));
        //         assertTrue(datum.get("exchange").equals("okex_spot"));
        //         assertTrue(datum.get("symbol").equals("PSTUSDT"));
        //         return null;
        // });

        // Valid<String> tableRowStringValidator = new Valid<String>((datum) -> {
        //         System.out.println(datum);
        //         assertNotNull(datum);
        //         return null;
        // });

        // long num = 1;

        // PAssert
        //     .that(results)
        //     .inOnTimePane(new IntervalWindow(baseTime, WINDOW_DURATION))
        //     .satisfies(tableRowValidator.getFn());

        // PAssert.thatSingleton(results.apply("Count", Count.globally()))
        //     .inOnTimePane(new IntervalWindow(baseTime, WINDOW_DURATION))
        //     .isEqualTo(num);

        // // PAssert.that(teamScores)
        //     .inOnTimePane(new IntervalWindow(baseTime, WINDOW_DURATION))
        //     .satisfies(tableRowValidator.getFn());
 
        // PAssert.that(results).satisfies(tableRowValidator.getFn());
        TEST_PIPELINE.run().waitUntilFinish();
    }

    
    private TimestampedValue<Event> event(TestEvent e, Duration baseTimeOffset) {

        Event event = new Event(
                e.getExchange(),
                e.getEventType(),
                e.getQuote(),
                e.getBase(),
                baseTime.plus(baseTimeOffset).getMillis(),
                e.getQuantity(),
                e.getPrice(),
                e.getSide()
        );

        return TimestampedValue.of(event, baseTime.plus(baseTimeOffset));
    }

}