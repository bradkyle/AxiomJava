package com.axiom.pipeline.datum;

import java.util.Map;
import com.google.common.base.MoreObjects;
import com.axiom.aggregator.util.StackTrace;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.TupleTag;
import org.joda.time.Instant;
import org.apache.beam.sdk.transforms.DoFn;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import org.apache.avro.reflect.Nullable;
import java.util.Arrays;

@DefaultCoder(AvroCoder.class)
public class Failure extends DoFn{

    // datum metadata
    private  Exception exception;
    private  String payload;

    // Private empty constructor used for reflection required by AvroIO.
    @SuppressWarnings("unused")
    private Failure() {}

    public Failure(
                   Exception exception,
                   String payload
                   ) {
        this.exception = exception;
        this.payload = payload;
    }

    public Exception getException() {
        return this.exception;
    }

    public void setException(Exception exception) {
        this.exception = exception;
    }

    public String getPayload() {
        return this.payload;
    }

    public void setPayload(String payload) {
        this.payload = payload;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("error_class", exception.getClass())
            .add("error_payload", exception.getMessage())
            .add("error_stack", StackTrace.getFullStackTrace(exception, '\n'))
            .add("payload", payload)
            .toString();
    }

}
