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
import com.axiom.pipeline.FeatureSchema;
import java.util.stream.Stream;

import com.axiom.pipeline.core.StatefulCombineToBigquery;

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
        options.setStreaming(true);

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
        Pipeline pipeline = Pipeline.create(options);

        // ==================================================================>
        // Trades
        // ==================================================================>
        // Read trades from trades directory on google cloud storage        

        PCollection<PubsubMessage> depthStream =
            pipeline
            .apply(
                   "Read Depth Events",
                   PubsubIO.readMessagesWithAttributes().fromTopic("depths")
            );

        PCollectionTuple depthResults = DepthParser.process(depthStream);
        PCollection<Row> validDepths = depthResults.get(DepthParser.VALID);
        PCollection<Failure> failedDepths = depthResults.get(DepthParser.FAILURE);

        // Read GenericRecord's of the given schema from files on GCS
        PCollection<PubsubMessage> tradeStream =
            pipeline
            .apply(
                   "Read Trade Events",
                   PubsubIO.readMessagesWithAttributes().fromTopic("trades")
            );

        PCollectionTuple tradeResults = TradeParser.process(tradeStream);
        PCollection<Row> validTrades = tradeResults.get(TradeParser.VALID);
        PCollection<Failure> failedTrades = tradeResults.get(TradeParser.FAILURE);

        PCollectionList<Row> collectionList = PCollectionList.of(validTrades).and(validDepths);
        PCollection<Row> mergedCollections = collectionList.apply(Flatten.<Row>pCollections());
        
        PCollection<TableRow> aggregated = mergedCollections.apply(
            "StatefulAggregationUSDTBSV", 
            new StatefulCombine(
                "okex_spot",
                "USDT",
                "BSV",
                Duration.standardSeconds(30),
                5
            )
        );

        percepts
            .apply(
                "Write PubSub Events", 
                PubsubIO.writeMessages().to(options.getOutputTopic())
            );
        
        // // Read GenericRecord's of the given schema from files on GCS
        // PCollection<PubsubMessage> outputStream =
        // pipeline
        //     .apply(
        //         "Read Output Events",
        //         PubsubIO.readMessagesWithAttributes().fromTopic("output")
        //     );

        // PCollection<Row> outputStream = outputStream.apply(
        //     "EnrichOutputStream",
        //     new EnrichOutputStream()
        // );






        // write to file
        // Execute the pipeline and return the result.
        return p.run();
    }

}