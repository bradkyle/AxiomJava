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
import org.apache.beam.sdk.coders.AvroCoder;

import org.ta4j.core.BaseBar;
import org.ta4j.core.BaseTimeSeries;
import org.ta4j.core.Bar;
import org.ta4j.core.TimeSeries;

import com.axiom.pipeline.datum.Event;

public class TimeSeriesCombineFn extends CombineFn<Bar, TimeSeries, TimeSeries> {

    final int maxSize;

    public TimeSeriesCombineFn(
        int maxSize
    ) {
        this.maxSize = maxSize;
    }

    @Override
    public TimeSeries createAccumulator() {
        TimeSeries timeSeries = new BaseTimeSeries();
        timeSeries.setMaximumBarCount(this.maxSize);
        return timeSeries;
    }

    @Override
    public TimeSeries addInput(TimeSeries accumulator, Bar input) {
        accumulator.addBar(input);
        return accumulator;
    }

    @Override
    public TimeSeries mergeAccumulators(Iterable<TimeSeries> accumulators) {
        TimeSeries timeSeries = new BaseTimeSeries();
        for (TimeSeries t : accumulators) {
            List<Bar> bars = t.getBarData();
            for (Bar bar : bars) {
                timeSeries.addBar(bar);
            }
        }
        return timeSeries;
    }

    @Override
    public TimeSeries extractOutput(TimeSeries accumulator) {
        return accumulator;
    }

}
