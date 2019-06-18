package com.axiom.pipeline.io;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.axiom.pipeline.datum.Event;
import org.apache.beam.sdk.io.DynamicAvroDestinations;

import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.io.FileBasedSink;
import org.apache.beam.sdk.io.FileBasedSink.DynamicDestinations;
import org.apache.beam.sdk.io.fs.ResourceId;

import org.apache.beam.sdk.io.DefaultFilenamePolicy;
import org.apache.beam.sdk.io.FileBasedSink.FilenamePolicy;
import org.apache.beam.sdk.io.FileBasedSink.OutputFileHints;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.ResolveOptions.StandardResolveOptions;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.apache.avro.Schema;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.io.DefaultFilenamePolicy.Params;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.io.FileSystems;

import com.axiom.pipeline.io.WindowedFilenamePolicy;

// This is the user class that controls dynamic destinations for this avro write. The input to
// AvroIO.Write will be UserEvent, and we will be writing GenericRecords to the file (in order
// to have dynamic schemas). Everything is per userid, so we define a dynamic destination type
// of Integer.
public class EventDynamicDestinations extends DynamicAvroDestinations<Event, Event.Partition, Event> {
    private static final Logger logger = LoggerFactory.getLogger(EventDynamicDestinations.class);

    final String baseDir;

    public EventDynamicDestinations(
        String baseDir
    ) {
        this.baseDir = baseDir;
    }
    
    public Event.Partition getDestination(Event event) {
        return event.getPartition();
    }

    public Event formatRecord(Event event) {
        return event;
    }

    @Override
    public Event.Partition getDefaultDestination() {
        return new Event.Partition(
            "None",
            "None",
            "None",
            "None"
        );
    }

    @Override
    public Schema getSchema(Event.Partition partition) {
        return AvroCoder.of(Event.class).getSchema();
    }

    public FilenamePolicy getFilenamePolicy(Event.Partition partition) {

        String outputDirectory =  String.join(
            "/",
            baseDir,
            partition.getExchange(),
            partition.getQuoteAsset(),
            partition.getBaseAsset(),
            partition.getEventType()
        );

        ResourceId outputDest = FileSystems.matchNewResource(outputDirectory, true);

        return DefaultFilenamePolicy.fromParams(new Params().withBaseFilename(outputDest));
      }


}
