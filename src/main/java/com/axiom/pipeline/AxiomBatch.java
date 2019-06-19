package com.axiom.pipeline;

import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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

import com.axiom.pipeline.datum.Event;

import org.apache.beam.sdk.transforms.Distinct;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.io.FileBasedSink;
import org.apache.beam.sdk.options.ValueProvider.NestedValueProvider;
import com.axiom.pipeline.io.EventDynamicDestinations;
import com.axiom.pipeline.core.StatefulCombineToBigquery;

import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;

import com.axiom.pipeline.util.Duration;

public class AxiomBatch {
    private static final Logger logger = LoggerFactory.getLogger(AxiomBatch.class);

    public interface Options extends PipelineOptions, StreamingOptions, GcpOptions {

        @Description("The Quote asset to use")
        @Required
        ValueProvider<String> getQuote();

        void setQuote(ValueProvider<String> value);

        @Description("The Base asset to use")
        @Required
        ValueProvider<String> getBase();

        void setBase(ValueProvider<String> value);

        @Description("The exchange to process")
        @Required
        ValueProvider<String> getExchange();

        void setExchange(ValueProvider<String> value);

        @Description("The directory from which avro files will be collected. Must end with a slash.")
        @Required
        ValueProvider<String> getInputDirectory();

        void setInputDirectory(ValueProvider<String> value);

        @Description("The output Bigquery dataset to output data to.")
        @Default.String("aggregations")
        ValueProvider<String> getOutputDataset();

        void setOutputDataset(ValueProvider<String> value);

        @Description("The maximum number of levels to aggregate")
        @Default.Integer(5)
        Integer getNumLevels();

        void setNumLevels(Integer value);

        @Description(
            "The window duration in which data will be written. Defaults to 5m. "
                + "Allowed formats are: "
                + "Ns (for seconds, example: 5s), "
                + "Nm (for minutes, example: 12m), "
                + "Nh (for hours, example: 2h).")
        @Default.String("5m")
        String getWindowDuration();

        void setWindowDuration(String value);

    }

    public static void main(String[] args) {
        Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
        run(options);
    }

    public static PipelineResult run(Options options) {
        // Create the pipeline
        Pipeline p = Pipeline.create(options);

        String inputDir = String.join(
            "/",
            options.getInputDirectory().get(),
            options.getExchange().get(),
            options.getQuote().get(),
            options.getBase().get(),
            "*"
        );

        System.out.println(inputDir);

        PCollection<Event> eventStream =
            p.apply(
                AvroIO.read(Event.class)
                .from(inputDir)
                .withHintMatchesManyFiles()
            );

        eventStream.apply(
            "StatefulAggregationUSDTBSV", 
            new StatefulCombineToBigquery(
                options.getProject(),
                options.getOutputDataset().get(),
                options.getExchange().get(),
                options.getQuote().get(),
                options.getBase().get(),
                Duration.parseDuration(options.getWindowDuration()),                
                options.getNumLevels()
            )
        );
        
        // write to file
        // Execute the pipeline and return the result.
        return p.run();
    }

}