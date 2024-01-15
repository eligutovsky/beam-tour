package com.example.aggregations;

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
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;

public class SumAggregation {

    private static final Logger LOG = LoggerFactory.getLogger(SumAggregation.class);

    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
        Pipeline pipeline = Pipeline.create(options);

        // Create input PCollection
        PCollection<KV<Integer, Integer>> input = pipeline.apply(
                Create.of(KV.of(1, 11),
                        KV.of(1, 36),
                        KV.of(2, 91),
                        KV.of(3, 33),
                        KV.of(3, 11),
                        KV.of(4, 33)));

        // The applyTransform() converts [input] to [output]
        PCollection<KV<Integer, Integer>> output = applyTransform(input);

        output.apply("Log", ParDo.of(new LogOutput<>("PCollection sum value")));

        pipeline.run();
    }

    // Sum.integersGlobally() to return the globally sum from `PCollection`
    static PCollection<KV<Integer, Integer>> applyTransform(PCollection<KV<Integer, Integer>> input) {
        return input.apply(Sum.integersPerKey());
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
