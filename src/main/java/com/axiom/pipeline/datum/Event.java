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
public class Event extends DoFn{

    private String exchange;
    private String eventType;
    private String quoteAsset;
    private String baseAsset;
    private Long time;
    private Double quantity;
    private Double price;
    private String side;

    private Schema schema = Schema.builder()
        .addStringField("event_type")
        .addStringField("exchange")
        .addStringField("quote_asset")
        .addStringField("base_asset")
        .addInt64Field("time")
        .addDoubleField("quantity")
        .addDoubleField("price")
        .addStringField("side")
        .build();

    // Private empty constructor used for reflection required by AvroIO.
    @SuppressWarnings("unused")
    private Event() {}

    public Event(
        String exchange,
        String eventType,
        String quoteAsset,
        String baseAsset,
        Long time,
        Double quantity,
        Double price,
        String side
    ) {
        this.exchange=exchange;
        this.eventType=eventType;
        this.quoteAsset=quoteAsset;
        this.baseAsset=baseAsset;
        this.time=time;
        this.quantity=quantity;
        this.price=price;
        this.side=side;
    }

    public String getExchange() {
        return this.exchange;
    }

    public void setExchange(String exchange) {
        this.exchange = exchange;
    }

    public String getEventType() {
        return this.eventId;
    }

    public void setEventType(String eventType) {
        this.eventType = eventType;
    }

    public String getQuoteAsset() {
        return this.quoteAsset;
    }

    public void setQuoteAsset(String quoteAsset) {
        this.quoteAsset = quoteAsset;
    }

    public String getBaseAsset() {
        return this.quoteAsset;
    }

    public void setBaseAsset(String baseAsset) {
        this.baseAsset = baseAsset;
    }

    public String getSide() {
        return this.side;
    }

    public void setSide(String side) {
        this.side = side;
    }

    public Long getTime() {
        return this.time;
    }

    public void setTime(Long time) {
        this.time = time;
    }

    public Double getQuantity() {
        return this.quantity;
    }

    public void setQuantity(Double quantity) {
        this.quantity = quantity;
    }

    public Double getPrice() {
        return this.price;
    }

    public void setPrice(Double price) {
        this.price = price;
    }

    public Partition getPartition() {
        return new Partition(
            this.getExchange(),
            this.getEventType(),
            this.getQuoteAsset(),
            this.getBaseAsset()
        );
    }

    public class Partition {
        public Partiton(
            String exchange,
            String eventType,
            String quoteAsset,
            String baseAsset
        ) {
            this.exchange=exchange;
            this.eventType=eventType;
            this.quoteAsset=quoteAsset;
            this.baseAsset=baseAsset;
        }

        public String getExchange() {
            return this.exchange;
        }
    
        public String getQuoteAsset() {
            return this.quoteAsset;
        }
    
        public String getBaseAsset() {
            return this.quoteAsset;
        }

        public String getEventType() {
            return this.eventId;
        }
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("eventId",eventId)
            .add("partitionId",partitionId)
            .add("exchange",exchange)
            .add("eventType",eventType)
            .add("quoteAsset",quoteAsset)
            .add("baseAsset",baseAsset)
            .add("symbol",symbol)
            .add("time",time)
            .add("quantity",quantity)
            .add("price",price)
            .add("side",side)
            .toString();
    }

    public Row toRow() {
        return Row.withSchema(schema)
            .addValues(
                this.getEventType(),
                this.getExchange(),
                this.getQuoteAsset(),
                this.getBaseAsset(),
                this.getTime(),
                this.getQuantity(),
                this.getPrice(),
                this.getSide()
            ).build();
    }

}
