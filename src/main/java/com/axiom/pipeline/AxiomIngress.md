package com.axiom.pipeline;

import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.values.Row;
import org.joda.time.Instant;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.WithTimestamps;
import com.axiom.pipeline.Options;
import com.axiom.pipeline.datum.Event;
import com.axiom.pipeline.parsers.TradeParser;
import com.axiom.pipeline.parsers.DepthParser;
import com.axiom.pipeline.datum.Failure;
import com.axiom.pipeline.io.EventDynamicDestinations;
import org.apache.beam.sdk.transforms.Distinct;
import org.apache.beam.sdk.io.FileBasedSink;
import org.apache.beam.sdk.options.ValueProvider.NestedValueProvider;


public class AxiomIngress {
    private static final Logger logger = LoggerFactory.getLogger(AxiomIngress.class);

    /**
     * Main entry point for executing the pipeline.
     *
     * @param args The command-line arguments to the pipeline.
     */
    public static void main(String[] args) {

        Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);

        run(options);
    }

    public static PipelineResult run(Options options) {
        // Create the pipeline
        Pipeline p = Pipeline.create(options);

        // ==================================================================>
        // LevelUpdates
        // ==================================================================>
        // Read trades from trades directory on google cloud storage        

        PCollection<PubsubMessage> depthStream = p.apply(
            "Read Depth Events",
            PubsubIO.readMessagesWithAttributes().fromTopic(options.getInputTopic())
        );

        PCollectionTuple depthResults = DepthParser.process(depthStream);
        PCollection<Event> validDepths = depthResults.get(DepthParser.VALID);
        PCollection<Failure> failedDepths = depthResults.get(DepthParser.FAILURE);

        // ==================================================================>
        // Trades
        // ==================================================================>
        // Read trades from trades directory on google cloud storage        
        
        PCollection<PubsubMessage> tradeStream = p.apply(
            "Read Depth Events",
            PubsubIO.readMessagesWithAttributes().fromTopic(options.getInputTopic())
        );
        
        PCollectionTuple tradeResults = TradeParser.process(tradeStream);
        PCollection<Event> validTrades = tradeResults.get(TradeParser.VALID);
        PCollection<Failure> failedTrades = tradeResults.get(TradeParser.FAILURE);

        PCollectionList<Event> collectionList = PCollectionList.of(validTrades).and(validDepths);
        PCollection<Event> mergedCollections = collectionList.apply(Flatten.<Event>pCollections());

        mergedCollections.apply(
            "AddTimestamps",
            WithTimestamps.of((Event event) -> new Instant(event.getTime()))
        ).apply(
            "WindowEvents",
            Window.<Row>into(FixedWindows.of(interval))
        ).apply(
            "FilterDuplicateEvents",
            Distinct.<Event>create()
        ).apply(
            "WriteEventsToAvro", 
            AvroIO.<Integer>write(Event.class)
            .to(new EventDynamicDestinations(
                options.getOutputDirectory()
            ))
            .withTempDirectory(
                NestedValueProvider.of(
                    options.getAvroTempDirectory(),
                    (SerializableFunction<String, ResourceId>) input -> FileBasedSink.convertToFileResourceIfPossible(input)
                )
            )
            .withWindowedWrites()
            .withNumShards(options.getNumShards())
        );
        
        // write to file
        // Execute the pipeline and return the result.
        return p.run();
    }

}