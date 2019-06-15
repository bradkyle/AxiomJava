package com.axiom.pipeline;

import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.options.ValueProvider;


public interface Options extends PipelineOptions, StreamingOptions {
    @Description("The Cloud Pub/Sub topic to read from.")
    @Required
    ValueProvider<String> getInputTopic();

    void setInputTopic(ValueProvider<String> value);

    @Description("The directory from which avro files will be collected. Must end with a slash.")
    @Required
    ValueProvider<String> getInputDirectory();

    void setInputDirectory(ValueProvider<String> value);

    @Description("The output Bigquery dataset to output data to.")
    @Required
    ValueProvider<String> getOutputDataset();

    void setOutputDataset(ValueProvider<String> value);

    @Description("The directory to output files to. Must end with a slash.")
    @Required
    ValueProvider<String> getFailureDirectory();

    void setFailureDirectory(ValueProvider<String> value);

    @Description("The filename prefix of the files to write to.")
    @Default.String("output")
    ValueProvider<String> getOutputFilenamePrefix();

    void setOutputFilenamePrefix(ValueProvider<String> value);

    @Description("The suffix of the files to write.")
    @Default.String("")
    ValueProvider<String> getOutputFilenameSuffix();

    void setOutputFilenameSuffix(ValueProvider<String> value);

    @Description(
        "The shard template of the output file. Specified as repeating sequences "
            + "of the letters 'S' or 'N' (example: SSS-NNN). These are replaced with the "
            + "shard number, or number of shards respectively")
    @Default.String("W-P-SS-of-NN")
    ValueProvider<String> getOutputShardTemplate();

    void setOutputShardTemplate(ValueProvider<String> value);

    @Description("The maximum number of output shards produced when writing.")
    @Default.Integer(1)
    Integer getNumShards();

    void setNumShards(Integer value);

    @Description(
        "The window duration in which data will be written. Defaults to 5m. "
            + "Allowed formats are: "
            + "Ns (for seconds, example: 5s), "
            + "Nm (for minutes, example: 12m), "
            + "Nh (for hours, example: 2h).")
    @Default.String("5m")
    String getOutputWriteWindowDuration();

    void setOutputWriteWindowDuration(String value);

    @Description(
        "The window duration in which data will be written. Defaults to 5m. "
            + "Allowed formats are: "
            + "Ns (for seconds, example: 5s), "
            + "Nm (for minutes, example: 12m), "
            + "Nh (for hours, example: 2h).")
    @Default.String("30m")
    String getFailureWriteWindowDuration();

    void setFailureWriteWindowDuration(String value);

    @Description("The Avro Write Temporary Directory. Must end with /")
    @Required
    ValueProvider<String> getAvroTempDirectory();

    void setAvroTempDirectory(ValueProvider<String> value);

}