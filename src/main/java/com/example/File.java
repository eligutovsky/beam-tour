// Copyright 2022 Google LLC
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

package com.example;

import java.util.Arrays;
import java.util.List;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;

public class File {
	public interface Options extends StreamingOptions {
		@Description("Input text to print.")
		@Default.String("My input text")
		String getInputText();

		void setInputText(String value);

		@Default.String("/Users/eli_g/Developer/beam-starter-java/sample1000.csv")
		String getInputFile();

		void setInputFile(String value);
	}

	public static PCollection<String> buildPipeline(Pipeline pipeline, String inputFile) {
		return pipeline
				.apply("Read file", TextIO.read().from(inputFile))
				.apply("Print elements",
						MapElements.into(TypeDescriptors.strings()).via(x -> {
							System.out.println(x);
							return x;
						}));
	}

	public static void main(String[] args) {
		var options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
		var pipeline = Pipeline.create(options);
		File.buildPipeline(pipeline, options.getInputFile());
		pipeline.run().waitUntilFinish();
	}
}