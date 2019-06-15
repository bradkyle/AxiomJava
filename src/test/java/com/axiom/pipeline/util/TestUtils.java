/*
 * Copyright (C) 2018 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.axiom.pipeline.util;

import com.google.common.io.ByteStreams;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.List;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.util.MimeTypes;
import java.io.Serializable;
import java.util.function.Function;
import java.io.Serializable;
import java.util.function.Function;
import org.joda.time.Instant;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.testing.TestPipeline;
import java.util.Map;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.DoFn;
import java.nio.file.Files;
import java.nio.file.Paths;
import com.axiom.pipeline.util.Json;
import java.util.Map;
import com.fasterxml.jackson.core.type.TypeReference;

import static org.junit.Assert.assertEquals;

/**
 * The {@link TestUtils} class provides common utilities
 * used for executing the unit tests.
 */
public class TestUtils {

    /**
     * Helper to generate files for testing.
     *
     * @param filePath The path to the file to write.
     * @param lines The lines to write.
     * @return The file written.
     * @throws IOException If an error occurs while creating or writing the file.
     */
    public static ResourceId writeToFile(
                                         String filePath, List<String> lines) throws IOException {

        return writeToFile(filePath, lines, Compression.UNCOMPRESSED);
    }

    /**
     * Helper to generate files for testing.
     *
     * @param filePath The path to the file to write.
     * @param lines The lines to write.
     * @param compression The compression type of the file.
     * @return The file written.
     * @throws IOException If an error occurs while creating or writing the file.
     */
    public static ResourceId writeToFile(
                                         String filePath, List<String> lines, Compression compression) throws IOException {

        String fileContents = String.join(System.lineSeparator(), lines);

        ResourceId resourceId = FileSystems.matchNewResource(filePath, false);

        String mimeType =
            compression == Compression.UNCOMPRESSED ? MimeTypes.TEXT : MimeTypes.BINARY;

        // Write the file contents to the channel and close.
        try (ReadableByteChannel readChannel =
             Channels.newChannel(new ByteArrayInputStream(fileContents.getBytes()))) {
            try (WritableByteChannel writeChannel =
                 compression.writeCompressed(FileSystems.create(resourceId, mimeType))) {
                ByteStreams.copy(readChannel, writeChannel);
            }
        }

        return resourceId;
    }

    // A default payload containing all fields that should not
    public static String readFile(String filename) {
        try {
            return new String(Files.readAllBytes(Paths.get(filename)));
        } catch(Exception e) {
            e.getStackTrace();
            return null;
        }
    }

    @FunctionalInterface
    public interface Validator<I> extends Serializable {
        Void  validate(I i);
    }

    /**
     * Helper to validate a datum output of a pipeline.
     *
     */
    public static class ValidCount<E> implements Serializable{
        private final int size;
        private final ValidFn<E> fn;

        public ValidCount(int size) {
            this.size = size;
            this.fn = new ValidFn<E>();
        }

        public ValidFn<E> getFn(){
            return this.fn;
        }

        public class ValidFn<T extends E>  implements SerializableFunction<Iterable<T>, Void> {
            @Override
            public Void apply(Iterable<T> input) {
                int count = 0;
                assertEquals(size, count);
                return null;
            };
        }
   }

    /**
     * Helper to validate a datum output of a pipeline.
     *
     */
    public static class ValidWithCount<E> implements Serializable{
        private final int size;
        private final ValidFn<E> fn;
        private final Validator<E> validator;

        public ValidWithCount(int size, Validator<E> validator) {
            this.size = size;
            this.validator = validator;
            this.fn = new ValidFn<E>();
        }

        public ValidFn<E> getFn(){
            return this.fn;
        }

        public class ValidFn<T extends E>  implements SerializableFunction<Iterable<T>, Void> {
            @Override
            public Void apply(Iterable<T> input) {
                int count = 0;
                for (E datum: input) {
                    count++;
                    validator.validate(datum);
                }
                assertEquals(size, count);
                return null;
            };
        }
   }

    /**
     * Helper to validate a datum output of a pipeline.
     *
     */
    public static class Valid<E> implements Serializable{
        private final ValidFn<E> fn;
        private final Validator<E> validator;

        public Valid(Validator<E> validator) {
            this.validator = validator;
            this.fn = new ValidFn<E>();
        }

        public ValidFn<E> getFn(){
            return this.fn;
        }

        public class ValidFn<T extends E>  implements SerializableFunction<Iterable<T>, Void> {
            @Override
            public Void apply(Iterable<T> input) {
                for (E datum: input) {
                    validator.validate(datum);
                }
                return null;
            };
        }
    }



}
