package com.example.transforms.combine;

/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

import org.apache.beam.learning.katas.util.Log;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Combine.BinaryCombineFn;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;

public class CombinePerKeyFunction {

    private static final Logger LOG = LoggerFactory.getLogger(CombinePerKeyFunction.class);

    static final String PLAYER_1 = "Player 1";
    static final String PLAYER_2 = "Player 2";
    static final String PLAYER_3 = "Player 3";

    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
        Pipeline pipeline = Pipeline.create(options);

        PCollection<KV<String, String>> input = pipeline
                .apply("ParseCitiesToTimeKV", Create.of(
                        KV.of("a", "apple"),
                        KV.of("o", "orange"),
                        KV.of("a", "avocado"),
                        KV.of("l", "lemon"),
                        KV.of("l", "limes")));
        PCollection<KV<String, String>> output = applyTransform(input);

        output.apply(ParDo.of(new LogOutput<>()));

        pipeline.run();
    }

    static PCollection<KV<String, String>> applyTransform(PCollection<KV<String, String>> input) {
        return input.apply(Combine.perKey(new SumStringBinaryCombineFn()));
    }

    static class SumStringBinaryCombineFn extends BinaryCombineFn<String> {

        @Override
        public String apply(String left, String right) {
            return left + "," + right;
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