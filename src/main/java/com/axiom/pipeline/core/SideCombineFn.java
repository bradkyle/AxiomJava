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

public class SideCombineFn extends CombineFn<Row, Map<Double, Double>, Map<Double, Double>> {

    final int numLevels;
    final String side;

    public SideCombineFn(
        int numLevels,
        String side
    ) {
        this.numLevels=numLevels;
        this.side=side;
    }

    @Override
    public Map<Double, Double> createAccumulator() {
        return new HashMap(); 
    }

    @Override
    public Map<Double, Double> addInput(Map<Double, Double> accumulator, Row input) {
        if (Double.compare(input.getDouble("quantity"), 0.000000001) < 0) {
            accumulator.remove(input.getDouble("price"));
        } else {
            accumulator.put(input.getDouble("price"), input.getDouble("quantity"));
        }
        return accumulator;
    }

    @Override
    public Map<Double, Double> mergeAccumulators(Iterable<Map<Double, Double>> accumulators) {
        Map<Double, Double> result = createAccumulator();
        for (Map<Double, Double> value : accumulators) {
        result.putAll(value);
        }
        return result;
    }

    @Override
    public Map<Double, Double> extractOutput(Map<Double, Double> accumulator) {
        return accumulator;
    }
}