


// This is the user class that controls dynamic destinations for this avro write. The input to
// AvroIO.Write will be UserEvent, and we will be writing GenericRecords to the file (in order
// to have dynamic schemas). Everything is per userid, so we define a dynamic destination type
// of Integer.
class EventDynamicDestinations extends DynamicAvroDestinations<Event, Event.Partition, Event> {

    final String baseDir;
    final String recordType;

    public EventDynamicDestinations() {
        this.baseDir = baseDir;
        this.recordType = recordType;
    }
    
    public Event.Partition getDestination(Event event) {
        return event.getPartition();
    }

    public WindowedFilenamePolicy getFilenamePolicy(Event.Partition partition) {

        String outputDirectory =  String.join(
            "/",
            baseDir,
            partition.getExchange(),
            partition.getQuoteAsset(),
            partition.getBaseAsset(),
            partition.getEventType()
        )

        return new WindowedFilenamePolicy(
            outputDirectory,
            "output",
            "W-P-SS-of-NN",
            ".avro"
        )

    }

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

}
