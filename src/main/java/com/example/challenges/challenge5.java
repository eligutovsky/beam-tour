package com.example.challenges;

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
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.beam.sdk.transforms.Flatten;

import java.io.Serializable;
import java.util.*;

public class challenge5 {

    private static final Logger LOG = LoggerFactory.getLogger(challenge5.class);

    public static void main(String[] args) {
        LOG.info("Running Task");

        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
        Pipeline pipeline = Pipeline.create(options);

        PCollection<String> input = pipeline
                .apply(TextIO.read().from("gs://apache-beam-samples/shakespeare/kinglear.txt"))
                .apply(FlatMapElements.into(TypeDescriptors.strings())
                        .via((String line) -> Arrays.asList(line.split("[^\\p{L}]+"))))
                .apply(Filter.by((String word) -> !word.isEmpty()));

        final PTransform<PCollection<String>, PCollection<Iterable<String>>> sample = Sample.fixedSizeGlobally(100);

        PCollection<String> limitedPCollection = input.apply(sample).apply(Flatten.iterables());

        groupWordsByFirstLetter(limitedPCollection).apply(ParDo.of(new LogOutput<>()));

        pipeline.run();
    }

    static PCollection<Map<String, List<String>>> groupWordsByFirstLetter(PCollection<String> input) {
        return input.apply(Combine.globally(new GroupWordByFirstLetterFn()));
    }

    static class GroupWordByFirstLetterFn
            extends Combine.CombineFn<String, GroupWordByFirstLetterFn.WordAccum, Map<String, List<String>>> {

        static class WordAccum implements Serializable {
            List<Map<String, String>> current = new ArrayList<>();
            Map<String, List<String>> result = new HashMap<>();

            public void add(String word) {
                String firstLetter = word.substring(0, 1);
                if (result.containsKey(firstLetter)) {
                    result.get(firstLetter).add(word);
                } else {
                    result.put(firstLetter, new ArrayList<>(Collections.singletonList(word)));
                }
            }

            public WordAccum mergeWith(WordAccum accum) {
                for (Map.Entry<String, List<String>> entry : accum.result.entrySet()) {
                    String key = entry.getKey();
                    List<String> value = entry.getValue();
                    if (result.containsKey(key)) {
                        result.get(key).addAll(value);
                    } else {
                        result.put(key, value);
                    }
                }
                return this;
            }
        }

        @Override
        public WordAccum createAccumulator() {
            return new WordAccum();
        }

        @Override
        public WordAccum addInput(WordAccum mutableAccumulator, String input) {
            mutableAccumulator.add(input);
            return mutableAccumulator;
        }

        @Override
        public WordAccum mergeAccumulators(Iterable<WordAccum> accumulators) {
            WordAccum merged = createAccumulator();
            for (WordAccum accum : accumulators) {
                merged.mergeWith(accum);
            }
            return merged;
        }

        @Override
        public Map<String, List<String>> extractOutput(WordAccum accumulator) {
            return accumulator.result;
        }

    }

    static class LogOutput<T> extends DoFn<T, T> {
        private final String prefix;

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