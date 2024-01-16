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

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Arrays;
import org.apache.beam.sdk.values.TypeDescriptors;

public class AdditionalOutput {

    private static final Logger LOG = LoggerFactory.getLogger(AdditionalOutput.class);

    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
        Pipeline pipeline = Pipeline.create(options);

        // List of elements
        PCollection<String> input = pipeline.apply(Create.of("Apache Beam is an open source unified programming model",
                "To define and execute data processing pipelines", "Go SDK"));

        PCollection<String> words = input
                .apply(FlatMapElements.into(TypeDescriptors.strings())
                        .via((String line) -> Arrays.asList(line.split("[^\\p{L}]+"))));

        TupleTag<String> upperCaseTag = new TupleTag<String>() {
        };
        TupleTag<String> lowerCaseTag = new TupleTag<String>() {
        };

        PCollectionTuple outputTuple = applyTransform(words, upperCaseTag, lowerCaseTag);

        outputTuple.get(upperCaseTag).apply("Uppercase: ",
                ParDo.of(new LogOutput<String>("Uppercase word: ")));

        outputTuple.get(lowerCaseTag).apply("Lowercase: ", ParDo.of(new LogOutput<String>("Lowercase word: ")));

        pipeline.run();
    }

    // The function has multiple outputs, numbers above 100 and below
    static PCollectionTuple applyTransform(
            PCollection<String> input, TupleTag<String> upperCase,
            TupleTag<String> lowerCase) {

        return input.apply(ParDo.of(new DoFn<String, String>() {

            @ProcessElement
            public void processElement(@Element String element, MultiOutputReceiver out) {
                if (element.equals(element.toLowerCase())) {
                    // First PCollection
                    out.get(lowerCase).output(element);
                } else {
                    // Additional PCollection
                    out.get(upperCase).output(element);
                }
            }

        }).withOutputTags(lowerCase, TupleTagList.of(upperCase)));
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