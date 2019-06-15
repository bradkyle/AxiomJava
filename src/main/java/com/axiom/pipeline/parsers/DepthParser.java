package com.axiom.pipeline.parsers;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.axiom.pipeline.datum.Failure;
import com.axiom.pipeline.util.Json.JsonException;
import com.axiom.pipeline.util.Json;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.Arrays;
import org.apache.beam.sdk.schemas.Schema;
import com.axiom.pipeline.datum.Event;
import com.axiom.pipeline.datum.AvroPubsubMessage;

public class DepthParser extends DoFn<AvroPubsubMessage, Event> {
    private static final Logger logger = LoggerFactory.getLogger(DepthParser.class);

     public static TupleTag<Event> VALID = new TupleTag<Event>(){};
     public static TupleTag<Failure> FAILURE = new TupleTag<Failure>(){};
     
     public static PCollectionTuple process(PCollection<AvroPubsubMessage> messages) {
         return messages
             .apply(
                    "Parse Depth Records",
                     ParDo.of(new DoFn<AvroPubsubMessage, Event>() {
                        @ProcessElement
                        public void processElement(ProcessContext context) {
                            try {
                                AvroPubsubMessage msg = context.element();
                                HashMap<String,Object> depth = Json.serializer().toMap(
                                                msg.getPayload()
                                );
                                gen_level_updates(depth, "ask", context, schema);
                                gen_level_updates(depth, "bid", context, schema);                                    

                            } catch (JsonException e) {
                                fail(e, context);
                            } catch (NullPointerException e) {
                                fail(e, context);
                            } catch (Exception e) {
                                fail(e, context);
                            }
                        }
                    }).withOutputTags(
                        VALID,
                        TupleTagList.of(FAILURE)
                    ));
     }

    protected static Void gen_level_updates(HashMap<String, Object> depth, String side, ProcessContext context, Schema schema) {
        ArrayList<HashMap<String, Object>> levels =  ((ArrayList<HashMap<String, Object>>) depth.get(side+"s"));
        if (levels.size() > 0) {
            for (HashMap<String, Object> level : levels) {
                Double price = new Double(level.get("price").toString());
                Double quantity = price * new Double(level.get("quantity").toString());
                context.output(new Event(
                    depth.get("exchange").toString(),
                    "levelUpdate",
                    depth.get("quote_asset").toString(),
                    depth.get("base_asset").toString(),
                    Long.parseLong(depth.get("event_time_ms").toString()),
                    quantity,
                    price,
                    side
                ));
            }
        }
        return null;
    }

    protected static Void fail(Exception e, ProcessContext c) {
        AvroPubsubMessage msg = c.element();
        Failure failure = new Failure(
            e, msg.toString()
        );
        logger.error(failure.toString());
        c.output(FAILURE, failure);
        return null;
    }
}
