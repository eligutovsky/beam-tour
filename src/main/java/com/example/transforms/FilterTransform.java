package com.example.transforms;

import java.util.Arrays;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
/*
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FilterTransform {

    private static final Logger LOG = LoggerFactory.getLogger(FilterTransform.class);

    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
        Pipeline pipeline = Pipeline.create(options);

        // Create input PCollection
        PCollection<Integer> input = pipeline.apply(Create.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10));

        // The [input] filtered with the applyTransform()
        PCollection<Integer> output = applyIntegerTransform(input);

        output.apply("Log", ParDo.of(new LogOutput<>("PCollection filtered value")));

        String str = "To be, or not to be: that is the question: Whether 'tis nobler in the mind to suffer The slings and arrows of outrageous fortune, Or to take arms against a sea of troubles, And by opposing end them. To die: to sleep";

        PCollection<String> stringInput = pipeline.apply(Create.of(str))
                .apply(FlatMapElements.into(TypeDescriptors.strings())
                        .via((String string) -> Arrays.asList(string.split("[^\\p{L}]+"))));
        PCollection<String> stringOutput = applyStringTransform(stringInput);

        stringOutput.apply("Log", ParDo.of(new LogOutput<>("PCollection filtered value")));

        pipeline.run();
    }

    // The method filters the collection so that the numbers are even
    static PCollection<Integer> applyIntegerTransform(PCollection<Integer> input) {
        return input.apply(Filter.by((Integer number) -> number % 2 == 0));
    }

    static PCollection<String> applyStringTransform(PCollection<String> input) {
        return input.apply(Filter.by((String word) -> word.startsWith(word.substring(0, 1).toUpperCase())));
    }

    static class LogOutput<T> extends DoFn<T, T> {
        private String prefix;

        LogOutput() {
            this.prefix = "Processing element";
        }

        LogOutput(String prefix) {
            this.prefix = prefix;
        }

        @ProcessElement
        public void processElement(ProcessContext c) throws Exception {
            LOG.info(prefix + ": {}", c.element());
        }
    }
}