
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


package com.axiom.aggregator.parsers;

import com.axiom.pipeline.util.Json;
import com.axiom.pipeline.parsers.TradeParser;

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

public class TradeParserTest {
    private static final Logger logger = LoggerFactory.getLogger(TradeParserTest.class);

    @Rule public final transient TestPipeline TEST_PIPELINE = TestPipeline.create();

    @Test
    @Category(NeedsRunner.class)
    public void testPubsubMessageToRow() throws Exception {

        ClassLoader classLoader = ClassLoader.getSystemClassLoader();

        File file = new File(classLoader.getResource("trade.json").getFile());
        System.out.println("File Found : " + file.exists());

        byte[] PAYLOAD = Files.readAllBytes(file.toPath());
    
        Map<String, String> attributes = ImmutableMap.of("id", "Ace");

        AvroPubsubMessage message = new AvroPubsubMessage(PAYLOAD, attributes);
        Instant timestamp = Instant.now();

        PCollection<AvroPubsubMessage> stream =
            TEST_PIPELINE
                .apply(Create.timestamped(TimestampedValue.of(message, timestamp)));

        PCollectionTuple results = TradeParser.process(stream);

        PCollection<Row> valid = results.get(TradeParser.VALID);
        PCollection<Failure> failures = results.get(TradeParser.FAILURE);

        Valid<Row> tradeValidator = new Valid<Row>((datum) -> {
                assertNotNull(datum.getString("symbol"));
                assertTrue(datum.getString("exchange").equals("okex_spot"));
                assertTrue(datum.getString("symbol").equals("XLMUSDT"));
                // assertNotNull(datum.getTimestamp());
                // assertNotNull(datum.getTweetId());
                // assertNotNull(datum.getTweetLang());
                // assertNotNull(datum.getRetweetCount());
                // assertNotNull(datum.getFavoriteCount());
                // assertNotNull(datum.getUserId());
                // assertNotNull(datum.getUserName());
                // assertNotNull(datum.getUserFollowersCount());
                // assertNotNull(datum.getUserFriendsCount());
                // assertNotNull(datum.getUserStatusesCount());
                // assertFalse(datum.getHashTags().isEmpty());
                // assertFalse(datum.getSymbols().isEmpty());
                return null;
        });

        // PAssert.that(valid).empty();
        PAssert.that(failures).empty();
        PAssert.that(valid).satisfies(tradeValidator.getFn());
        TEST_PIPELINE.run().waitUntilFinish();

        // PAssert.that(results)
        // .containsInAnyOrder(
        //     new AvroPubsubMessageRecord(payload, attributes, timestamp.getMillis()));

            // Run the pipeline.
    
    }

    @Test
    @Category(NeedsRunner.class)
    public void testPubsubMessageFromAvro() throws Exception {

        // ClassLoader classLoader = ClassLoader.getSystemClassLoader();

        // File file = new File(classLoader.getResource("trades.avro").getFile());
        // System.out.println("File Found : " + file.exists());

        // PCollection<AvroPubsubMessage> tradeStream =
        //         TEST_PIPELINE.apply(AvroIO.read(AvroPubsubMessage.class).from(file.toPath().toString()));

        // PCollectionTuple results = TradeParser.process(tradeStream);
        // PCollection<Row> valid = results.get(TradeParser.VALID);
        // PCollection<Failure> failures = results.get(TradeParser.FAILURE);

        

        
        // TEST_PIPELINE.run().waitUntilFinish();
    
    }
}