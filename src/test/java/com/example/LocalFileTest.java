package com.example;

import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.example.basics.LocalFile;

@RunWith(JUnit4.class)
public class LocalFileTest {
    @Rule
    public final transient TestPipeline pipeline = TestPipeline.create();

    @Test
    public void fileRuns() {
        var elements = LocalFile.buildPipeline(pipeline, "./sample1000.csv");

        // Check that elements not empty.
        PAssert.that(elements).satisfies(x -> {
            assert x.iterator().hasNext();
            return null;
        });
        pipeline.run().waitUntilFinish();
    }
}
