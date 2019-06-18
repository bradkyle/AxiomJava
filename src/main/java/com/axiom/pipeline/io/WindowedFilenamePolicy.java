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
import org.apache.beam.sdk.schemas.Schema;

public class WindowedFilenamePolicy extends FilenamePolicy {
    /** The logger to output status messages to. */
    private static final Logger LOG = LoggerFactory.getLogger(WindowedFilenamePolicy.class);

    private final String outputDirectory;
    private final String outputFilenamePrefix;
    private final String suffix;
    private final String shardTemplate;

    private static final DateTimeFormatter YEAR = DateTimeFormat.forPattern("YYYY");
    private static final DateTimeFormatter MONTH = DateTimeFormat.forPattern("MM");
    private static final DateTimeFormatter DAY = DateTimeFormat.forPattern("dd");
    private static final DateTimeFormatter HOUR = DateTimeFormat.forPattern("HH");

    public WindowedFilenamePolicy(
        String outputDirectory,
        String outputFilenamePrefix,
        String shardTemplate,
        String suffix
    ) {
        this.outputDirectory = outputDirectory;
        this.outputFilenamePrefix = outputFilenamePrefix;
        this.shardTemplate = shardTemplate;
        this.suffix = suffix;
    }

    @Override
    public ResourceId windowedFilename(
        int shardNumber,
        int numShards,
        BoundedWindow window,
        PaneInfo paneInfo,
        OutputFileHints outputFileHints
    ) {

        ResourceId outputFile = resolveWithDateTemplates(outputDirectory, window)
                .resolve(
                    outputFilenamePrefix, 
                    StandardResolveOptions.RESOLVE_FILE
                );

        DefaultFilenamePolicy policy =
            DefaultFilenamePolicy.fromStandardParameters(
                StaticValueProvider.of(outputFile), 
                shardTemplate, 
                suffix, 
                true
            );

        ResourceId result =
            policy.windowedFilename(
                shardNumber, 
                numShards, 
                window, 
                paneInfo, 
                outputFileHints
            );

        LOG.debug("Windowed file name policy created: {}", result.toString());
        return result;
    }

    @Override
    public ResourceId unwindowedFilename(
        int shardNumber, 
        int numShards, 
        OutputFileHints outputFileHints
    ) {
        throw new UnsupportedOperationException(
            "There is no windowed filename policy for "
                + "unwindowed file output. Please use the WindowedFilenamePolicy with windowed "
                + "writes or switch filename policies.");
    }

    private ResourceId resolveWithDateTemplates(
        String outputDirectoryStr,
        BoundedWindow window
    ) {
        ResourceId outputDirectory = FileSystems.matchNewResource(outputDirectoryStr, true);
        if (window instanceof IntervalWindow) {
            IntervalWindow intervalWindow = (IntervalWindow) window;
            DateTime time = intervalWindow.end().toDateTime();
            String outputPath = outputDirectory.toString();
            outputPath = outputPath.replace("YYYY", YEAR.print(time));
            outputPath = outputPath.replace("MM", MONTH.print(time));
            outputPath = outputPath.replace("DD", DAY.print(time));
            outputPath = outputPath.replace("HH", HOUR.print(time));
            outputDirectory = FileSystems.matchNewResource(outputPath, true);
        }
        return outputDirectory;
    }
}
