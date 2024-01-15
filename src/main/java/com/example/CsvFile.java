package com.example;

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

import java.util.Arrays;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;

import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.Sample;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CsvFile {

    private static final Logger LOG = LoggerFactory.getLogger(CsvFile.class);

    public static void main(String[] args) {

        PipelineOptions options = PipelineOptionsFactory.create();

        // Create the Pipeline object with the options we defined above
        Pipeline pipeline = Pipeline.create(options);
        PCollection<String> input = pipeline
                .apply(TextIO.read().from("gs://apache-beam-samples/nyc_taxi/misc/sample1000.csv"));

        PCollection<Double> rideTolalAmounts = input.apply(ParDo.of(new ExtractTaxiRideCostFn()));
        PCollection<Integer> ridePassengersCollection = input.apply(ParDo.of(new ExtractTaxiPassengersCountFn()));
        final PTransform<PCollection<Double>, PCollection<Iterable<Double>>> sampleDouble = Sample
                .fixedSizeGlobally(10);
        PCollection<Double> rideAmounts = rideTolalAmounts.apply(sampleDouble)
                .apply(Flatten.iterables())
                .apply("Log amounts", ParDo.of(new LogDouble()));

        final PTransform<PCollection<Integer>, PCollection<Iterable<Integer>>> sampleInteger = Sample
                .fixedSizeGlobally(10);
        PCollection<Integer> ridePassengers = ridePassengersCollection.apply(sampleInteger)
                .apply(Flatten.iterables())
                .apply("Log passengers", ParDo.of(new DoFn<Integer, Integer>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) throws Exception {
                        LOG.info("Passengers: {}", c.element());
                        c.output(c.element());
                    }
                }));

        pipeline.run().waitUntilFinish();

    }

    private static Double tryParseTaxiRideCost(String[] inputItems) {
        try {
            return Double.parseDouble(tryParseString(inputItems, 16));
        } catch (NumberFormatException | NullPointerException e) {
            return 0.0;
        }
    }

    private static String tryParseString(String[] inputItems, int index) {
        return inputItems.length > index ? inputItems[index] : null;
    }

    static class ExtractTaxiPassengersCountFn extends DoFn<String, Integer> {

        @ProcessElement
        public void processElement(ProcessContext c) {
            String[] items = c.element().split(",");
            Integer passengersCount = tryParseTaxiPassengersCount(items);
            c.output(passengersCount);
        }

        private Integer tryParseTaxiPassengersCount(String[] inputItems) {
            try {
                return Integer.parseInt(tryParseString(inputItems, 3));
            } catch (NumberFormatException | NullPointerException e) {
                return 0;
            }
        }
    }

    static class ExtractTaxiRideCostFn extends DoFn<String, Double> {

        @ProcessElement
        public void processElement(ProcessContext c) {
            String[] items = c.element().split(",");
            Double totalAmount = tryParseTaxiRideCost(items);
            c.output(totalAmount);
        }
    }

    public static class LogDouble extends DoFn<Double, Double> {

        @ProcessElement
        public void processElement(ProcessContext c) throws Exception {
            LOG.info("Total Amount: {}", c.element());
            c.output(c.element());
        }
    }
}
