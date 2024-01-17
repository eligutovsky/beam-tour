package com.example.transforms.schema;

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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.JavaFieldSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.annotations.SchemaCreate;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.util.StreamUtils;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Objects;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collections;
import java.util.List;
import java.io.Serializable;

public class CoderFunction {
    private static final Logger LOG = LoggerFactory.getLogger(CoderFunction.class);

    @DefaultSchema(JavaFieldSchema.class)
    public static class Game {
        public String userId;
        public Integer score;
        public String gameId;
        public String date;
        public Boolean winner;

        @SchemaCreate
        public Game(String userId, Integer score, String gameId, String date, Boolean winner) {
            this.userId = userId;
            this.score = score;
            this.gameId = gameId;
            this.date = date;
            this.winner = winner;
        }

        @Override
        public String toString() {
            return "Game{" +
                    "userId='" + userId + '\'' +
                    ", score='" + score + '\'' +
                    ", gameId='" + gameId + '\'' +
                    ", date='" + date + '\'' +
                    ", winner='" + winner + '\'' +
                    '}';
        }

        public static Boolean isWinner(Integer score) {
            return score >= 10 ? true : false;
        }
    }

    // User schema
    @DefaultSchema(JavaFieldSchema.class)
    public static class User {
        public String userId;
        public String userName;

        public Game game;

        @SchemaCreate
        public User(String userId, String userName, Game game) {
            this.userId = userId;
            this.userName = userName;
            this.game = game;
        }

        @Override
        public String toString() {
            return "User{" +
                    "userId='" + userId + '\'' +
                    ", userName='" + userName + '\'' +
                    ", game=" + game +
                    '}';
        }
    }

    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
        Pipeline pipeline = Pipeline.create(options);

        PCollection<User> input = getProgressPCollection(pipeline);

        input
                .setCoder(CustomCoder.of())
                .apply("User", ParDo.of(new LogOutput<>("User row")));

        pipeline.run();
    }

    public static PCollection<User> getProgressPCollection(Pipeline pipeline) {
        PCollection<String> rides = pipeline
                .apply(TextIO.read().from("gs://apache-beam-samples/game/small/gaming_data.csv"));
        final PTransform<PCollection<String>, PCollection<Iterable<String>>> sample = Sample.fixedSizeGlobally(10);
        return rides.apply(sample).apply(Flatten.iterables()).apply(ParDo.of(new ExtractGameStatisticsFn()));
    }

    static class ExtractGameStatisticsFn extends DoFn<String, User> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            String[] items = c.element().split(",");
            int score = Integer.parseInt(items[2]);
            boolean winner = Game.isWinner(score);
            c.output(new User(items[0], items[1], new Game(items[0], score, items[3], items[4], winner)));
        }
    }

    static class CustomCoder extends Coder<User> {
        private static final CustomCoder INSTANCE = new CustomCoder();

        public static CustomCoder of() {
            return INSTANCE;
        }

        @Override
        public void encode(User user, OutputStream outStream) throws IOException {
            String line = user.userId + "," + user.userName + ";" + user.game.score + "," + user.game.gameId + ","
                    + user.game.date + "," + user.game.winner;
            outStream.write(line.getBytes());
        }

        @Override
        public User decode(InputStream inStream) throws IOException {
            final String serializedDTOs = new String(StreamUtils.getBytesWithoutClosing(inStream));
            String[] params = serializedDTOs.split(";");
            String[] user = params[0].split(",");
            String[] game = params[1].split(",");
            int score = Integer.parseInt(game[0]);
            boolean winner = Game.isWinner(score);
            return new User(user[0], user[1],
                    new Game(user[0], score, game[1], game[2], winner));
        }

        @Override
        public List<? extends Coder<?>> getCoderArguments() {
            return Collections.emptyList();
        }

        @Override
        public void verifyDeterministic() {
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
