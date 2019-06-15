package com.axiom.pipeline.util;

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.core.type.TypeReference;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.List;
import java.util.HashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.core.JsonParser;

/**
 * This pipeline ingests incoming data from a Cloud Pub/Sub topic and outputs the raw data into
 * windowed Avro files at the specified output directory.
 *
 * <p>Files output will have the following schema:
 *
 * <pre>
 *   {
 *      "type": "record",
 *      "name": "AvroPubsubMessageRecord",
 *      "namespace": "com.google.cloud.teleport.avro",
 *      "fields": [
 *        {"name": "message", "type": {"type": "array", "items": "bytes"}},
 *        {"name": "attributes", "type": {"type": "map", "values": "string"}},
 *        {"name": "timestamp", "type": "long"}
 *      ]
 *   }
 * </pre>
 *
 * <p>Example Usage:
 *
 * <pre>
 * mvn compile exec:java \
 *   -Dexec.mainClass=org.apache.beam.examples.templates.${PIPELINE_NAME} \
 *   -Dexec.cleanupDaemonThreads=false \
 *   -Dexec.args=" \
 *   --project=${PROJECT_ID} \
 *   --stagingLocation=gs://${PROJECT_ID}/dataflow/pipelines/${PIPELINE_FOLDER}/staging \
 *   --tempLocation=gs://${PROJECT_ID}/dataflow/pipelines/${PIPELINE_FOLDER}/temp \
 *   --runner=DataflowRunner \
 *   --windowDuration=2m \
 *   --numShards=1 \
 *   --topic=projects/${PROJECT_ID}/topics/windowed-files \
 *   --outputDirectory=gs://${PROJECT_ID}/temp/ \
 *   --outputFilenamePrefix=windowed-file \
 *   --outputFilenameSuffix=.avro
 *   --avroTempDirectory=gs://${PROJECT_ID}/avro-temp-dir/"
 * </pre>
 */
public class Json {
    private static final Logger logger = LoggerFactory.getLogger(JsonParser.class);

    // {{start:setup}}
    private static final Json DEFAULT_SERIALIZER;
    static {

        ObjectMapper mapper = new ObjectMapper();

        // Don't throw an exception when json has extra fields you are
        // not serializing on. This is useful when you want to use a pojo
        // for deserialization and only care about a portion of the json
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        mapper.configure(SerializationFeature.WRAP_ROOT_VALUE, false);
        mapper.enable(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY);
        mapper.configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true);
        // mapper.configure(JsonParser.Feature.ALLOW_TRAILING_COMMA, true);
        // // mapper.configure(JsonParser.Feature.IGNORE_UNDEFINED, true);

        // Ignore null values when writing json.
        mapper.configure(SerializationFeature.WRITE_NULL_MAP_VALUES, false);
        mapper.setSerializationInclusion(Include.NON_NULL);

        DEFAULT_SERIALIZER = new Json(mapper);
    }
    // {{end:setup}}

    public static Json serializer() {
        return DEFAULT_SERIALIZER;
    }

    private final ObjectMapper mapper;
    private final ObjectWriter writer;
    private final ObjectWriter prettyWriter;

    // Only let this be called statically. Hide the constructor
    private Json(ObjectMapper mapper) {
        this.mapper = mapper;
        this.writer = mapper.writer();
        this.prettyWriter = mapper.writerWithDefaultPrettyPrinter();
    }

    public ObjectMapper mapper() {
        return mapper;
    }

    public ObjectWriter writer() {
        return writer;
    }

    public ObjectWriter prettyWriter() {
        return prettyWriter;
    }

    public HashMap<String,Object> toMap(byte[] bytes) {
        try {
            return mapper.readValue(bytes, HashMap.class);
        } catch (IOException e) {
            throw new JsonException(e);
        }
    }

    // {{start:fromBytes}}
    public <T> T fromJson(byte[] bytes, TypeReference<T> typeRef) {
        try {
            return mapper.readValue(bytes, typeRef);
        } catch (IOException e) {
            throw new JsonException(e);
        }
    }
    // {{end:fromBytes}}

    // {{start:readJson}}
    public <T> T fromJson(String json, TypeReference<T> typeRef) {
        try {
            return mapper.readValue(json, typeRef);
        } catch (IOException e) {
            throw new JsonException(e);
        }
    }
    // {{end:readJson}}

    public <T> List<T> fromJsonArray(byte[] bytes, TypeReference<List<T>> typeRef) {
        try {
            return mapper.readValue(bytes, typeRef);
        } catch (IOException e) {
            throw new JsonException(e);
        }
    }

    public <T> List<T> fromJsonArray(String json, TypeReference<List<T>> typeRef) {
        try {
            return mapper.readValue(json, typeRef);
        } catch (IOException e) {
            throw new JsonException(e);
        }
    }

    public <T> Map<String, T> fromJsonMap(byte[] bytes, TypeReference<Map<String, T>> typeRef) {
        try {
            return mapper.readValue(bytes, typeRef);
        } catch (IOException e) {
            throw new JsonException(e);
        }
    }

    public <T> Map<String, T> fromJsonMap(String json, TypeReference<Map<String, T>> typeRef) {
        try {
            return mapper.readValue(json, typeRef);
        } catch (IOException e) {
            throw new JsonException(e);
        }
    }

    // {{start:fromNode}}
    public <T> T fromNode(JsonNode node, TypeReference<T> typeRef) {
        try {
            return mapper.readValue(node.toString(), typeRef);
        } catch (IOException e) {
            throw new JsonException(e);
        }
    }
    // {{end:fromNode}}

    public <T> T fromObject(Object obj, TypeReference<T> typeRef) {
        try {
            return mapper.readValue(toString(obj), typeRef);
        } catch (IOException e) {
            throw new JsonException(e);
        }
    }

    // {{start:fromInputStream}}
    public <T> T fromInputStream(InputStream is, TypeReference<T> typeRef) {
        try {
            return mapper.readValue(is, typeRef);
        } catch (IOException e) {
            throw new JsonException(e);
        }
    }
    // {{end:fromInputStream}}

    // {{start:writeJson}}
    public String toString(Object obj) {
        try {
            return writer.writeValueAsString(obj);
        } catch (IOException e) {
            throw new JsonException(e);
        }
    }
    // {{end:writeJson}}

    // {{start:toPrettyString}}
    public String toPrettyString(Object obj) {
        try {
            return prettyWriter.writeValueAsString(obj);
        } catch (IOException e) {
            throw new JsonException(e);
        }
    }
    // {{end:toPrettyString}}


    public Map<String, Object> mapFromJson(byte[] bytes) {
        try {
            return mapper.readValue(bytes, new TypeReference<Map<String, Object>>() {
            });
        } catch (IOException e) {
            throw new JsonException(e);
        }
    }

    public Map<String, Object> mapFromJson(String json) {
        try {
            return mapper.readValue(json, new TypeReference<Map<String, Object>>() {
            });
        } catch (IOException e) {
            throw new JsonException(e);
        }
    }

    // {{start:jsonNode}}
    public JsonNode nodeFromJson(String json) {
        try {
            return mapper.readTree(json);
        } catch (IOException e) {
            throw new JsonException(e);
        }
    }
    // {{end:jsonNode}}

    public JsonNode nodeFromObject(Object obj) {
        try {
            return mapper.readTree(toString(obj));
        } catch (IOException e) {
            throw new JsonException(e);
        }
    }

    public static class JsonException extends RuntimeException {
        private JsonException(Exception ex) {
            super(ex);
        }
    }
}
