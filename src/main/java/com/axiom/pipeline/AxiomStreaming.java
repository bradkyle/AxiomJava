package com.axiom.pipeline;

import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.axiom.pipeline.Options;

import org.apache.avro.file.CodecFactory;

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
import java.util.stream.Stream;

import com.axiom.pipeline.core.StatefulCombineToBigquery;

import com.axiom.pipeline.datum.Event;
import com.axiom.pipeline.datum.AvroPubsubMessage;

public class AxiomStreaming {
    private static final Logger logger = LoggerFactory.getLogger(AxiomStreaming.class);

    private static final CodecFactory DEFAULT_CODEC = CodecFactory.deflateCodec(9);

    /**
     * Main entry point for executing the pipeline.
     *
     * @param args The command-line arguments to the pipeline.
     */
    public static void main(String[] args) {

        Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);

        run(options);
    }

    /**
       Schema schema = new Schema.Parser().parse(new File(options.getAvroSchemaPath()));
       * Runs the pipeline with the supplied options.
       *
       * @param options The execution parameters to the pipeline.
       * @return The result of the pipeline execution.
       */
    public static PipelineResult run(Options options) {
        // Create the pipeline
        Pipeline p = Pipeline.create(options);

        // ==================================================================>
        // LevelUpdates
        // ==================================================================>
        // Read trades from trades directory on google cloud storage        

        PCollection<AvroPubsubMessage> depthStream = p.apply(
            "Read Depth Events",
            PubsubIO.readMessagesWithAttributes().fromTopic("depths")
        ).apply("Map Depths to Archive", ParDo.of(new PubsubMessageToArchiveDoFn()));

        PCollectionTuple depthResults = DepthParser.process(depthStream);
        PCollection<Event> validDepths = depthResults.get(DepthParser.VALID);
        PCollection<Failure> failedDepths = depthResults.get(DepthParser.FAILURE);

        // ==================================================================>
        // Trades
        // ==================================================================>
        // Read trades from trades directory on google cloud storage        
        
        PCollection<AvroPubsubMessage> tradeStream = p.apply(
            "Read Trades Events",
            PubsubIO.readMessagesWithAttributes().fromTopic("trades")
        ).apply("Map Trades to Archive", ParDo.of(new PubsubMessageToArchiveDoFn()));
        
        PCollectionTuple tradeResults = TradeParser.process(tradeStream);
        PCollection<Event> validTrades = tradeResults.get(TradeParser.VALID);
        PCollection<Failure> failedTrades = tradeResults.get(TradeParser.FAILURE);

        // ==================================================================>
        // Merged
        // ==================================================================>
        // Read trades from trades directory on google cloud storage        

        PCollectionList<Event> collectionList = PCollectionList.of(validTrades).and(validDepths);
        PCollection<Event> mergedCollections = collectionList.apply(Flatten.<Event>pCollections());
        
        mergedCollections.apply(
            "StatefulAggregationUSDTBSV", 
            new StatefulCombineToBigquery(
                "axiom-239308",
                options.getOutputDataset(),
                "okex_spot",
                "USDT",
                "BSV",
                Duration.standardMinutes(5),
                3
            )
        ).apply("ConvertToString", ParDo.of(new DoFn<TableRow, String>() {
            @ProcessElement
            public void processElement(ProcessContext c) {
                c.output(c.element().toString());
            }
        })).apply(
            "Write Aggregated Features",
            PubsubIO.writeStrings().to("output")
        );
        
        // write to file
        // Execute the pipeline and return the result.
        return p.run();
    }

    static class PubsubMessageToArchiveDoFn extends DoFn<PubsubMessage, AvroPubsubMessage> {
        @ProcessElement
        public void processElement(ProcessContext context) {
          PubsubMessage message = context.element();
          context.output(new AvroPubsubMessage(message.getPayload(), message.getAttributeMap(), context.timestamp().getMillis()));
        }
    }

}