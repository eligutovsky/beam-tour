package com.example.transforms.map;

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
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.transforms.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ParDoOneToOne {

    private static final Logger LOG = LoggerFactory.getLogger(ParDoOneToOne.class);

    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
        Pipeline pipeline = Pipeline.create(options);

        // List of elements
        PCollection<Integer> input = pipeline.apply(Create.of(1, 2, 3, 4, 5));

        // The applyTransform() converts [input] to [output]
        PCollection<Integer> output = applyTransform(input);

        output.apply("Log", ParDo.of(new LogOutput<Integer>()));

        pipeline.run();
    }

    // The applyTransform() multiplies the number by 10 and outputs
    static PCollection<Integer> applyTransform(PCollection<Integer> input) {
        return input.apply(ParDo.of(new DoFn<Integer, Integer>() {

            @ProcessElement
            public void processElement(@Element Integer number, OutputReceiver<Integer> out) {
                out.output(number * 10);
            }

        }));
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