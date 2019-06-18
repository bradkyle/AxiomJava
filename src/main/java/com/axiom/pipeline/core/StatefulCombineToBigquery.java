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
import com.axiom.pipeline.core.FeatureSchema;
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
import java.util.Arrays;
import com.axiom.pipeline.datum.Event;

import com.axiom.pipeline.core.StatefulCombine;

public class StatefulCombineToBigquery extends PTransform<PCollection<Event>, PCollection<TableRow>> {
    private static final Logger logger = LoggerFactory.getLogger(StatefulCombine.class);

    final String projectId;
    final String datasetId;
    final String exchange;
    final String quote_asset;
    final String base_asset;
    final String symbol;
    final Duration interval;
    final TableReference tableSpec;
    final int numLevels;

    public StatefulCombineToBigquery(
        String projectId,
        ValueProvider<String> datasetId,
        String exchange,
        String quote_asset,
        String base_asset,
        Duration interval,
        int numLevels
    ) {
        this.datasetId=datasetId.get();
        this.projectId=projectId;
        this.exchange=exchange;
        this.quote_asset=quote_asset;
        this.base_asset=base_asset;
        this.symbol=base_asset+quote_asset;
        this.interval=interval;
        this.numLevels=numLevels;

        this.tableSpec =
            new TableReference()
                .setProjectId(projectId)
                .setDatasetId(datasetId.get())
                .setTableId(String.join("_", Arrays.asList(exchange, quote_asset, base_asset, interval.toString())));
    }

    @Override
    public PCollection<TableRow> expand(PCollection<Event> mergedCollections){

        PCollection<TableRow> aggregation = mergedCollections.apply(
                "StatefulAggregation", 
                new StatefulCombine(
                    "okex_spot",
                    "BTC",
                    "ETH",
                    Duration.standardSeconds(30),
                    5
                )
            );

         // To bigquery
        aggregation
          .apply(
            BigQueryIO.writeTableRows()
            .to(tableSpec)
            .withSchema(FeatureSchema.FEATURE_SCHEMA)
            .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
            .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE));

        return aggregation;
    }
}
