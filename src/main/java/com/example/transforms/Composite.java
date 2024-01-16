package com.example.transforms;

// Licensed to the Apache Software Foundation (ASF) under one or more

// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

import static org.apache.beam.sdk.values.TypeDescriptors.integers;
import static org.apache.beam.sdk.values.TypeDescriptors.strings;

import java.util.Arrays;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Composite {

    private static final Logger LOG = LoggerFactory.getLogger(Composite.class);

    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
        Pipeline pipeline = Pipeline.create(options);

        pipeline
                // List of elements
                .apply(Create.of("1,2,3,4,5", "6,7,8,9,10"))

                // Composite operation
                .apply(new ExtractAndMultiplyNumbers())

                .apply("Log", ParDo.of(new LogOutput<Integer>()));

        pipeline.apply(Create.of("quick brown fox jumps over the lazy dog aaa"))
                .apply(new CountLetters())
                .apply("Log", ParDo.of(new LogOutput<KV<String, Long>>("Counted letters: ")));

        pipeline.run();
    }

    static class CountLetters
            extends PTransform<PCollection<String>, PCollection<KV<String, Long>>> {

        // First operation
        @Override
        public PCollection<KV<String, Long>> expand(PCollection<String> input) {
            return input
                    .apply("ExtractWords", ParDo.of(new DoFn<String, String>() {
                        @ProcessElement
                        public void processElement(ProcessContext c) {
                            String[] words = c.element().split(" ");
                            for (String word : words) {
                                if (!word.equals("")) {
                                    c.output(word);
                                }
                            }
                        }
                    }))
                    .apply("ExtractCharacters",
                            FlatMapElements.into(strings()).via(word -> Arrays.asList(word.split(""))))
                    .apply("CountLetters", Count.perElement());
        }

    }

    // The class with PTransform
    static class ExtractAndMultiplyNumbers
            extends PTransform<PCollection<String>, PCollection<Integer>> {

        // First operation
        @Override
        public PCollection<Integer> expand(PCollection<String> input) {
            return input
                    .apply(ParDo.of(new DoFn<String, Integer>() {
                        // Second operation
                        @ProcessElement
                        public void processElement(@Element String numbers, OutputReceiver<Integer> out) {
                            Arrays.stream(numbers.split(","))
                                    .forEach(numStr -> out.output(Integer.parseInt(numStr)));
                        }

                    }))

                    .apply(MapElements.into(integers()).via(number -> number * 10));
        }

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