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
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.JavaFieldSchema;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.annotations.SchemaCreate;
import org.apache.beam.sdk.schemas.transforms.Convert;
import org.apache.beam.sdk.schemas.transforms.RenameFields;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.util.StreamUtils;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.io.Serializable;

public class ConvertFunction {
    private static final Logger LOG = LoggerFactory.getLogger(ConvertFunction.class);

    @DefaultSchema(JavaFieldSchema.class)
    public static class Game {
        public String userId;
        public Integer score;
        public String gameId;
        public String date;

        @SchemaCreate
        public Game(String userId, Integer score, String gameId, String date) {
            this.userId = userId;
            this.score = score;
            this.gameId = gameId;
            this.date = date;
        }

        @Override
        public String toString() {
            return "Game{" +
                    "userId='" + userId + '\'' +
                    ", score='" + score + '\'' +
                    ", gameId='" + gameId + '\'' +
                    ", date='" + date + '\'' +
                    '}';
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

        Schema gameSchema = Schema.builder()
                .addStringField("userId")
                .addInt32Field("score")
                .addStringField("gameId")
                .addStringField("date")
                .build();

        Schema schema = Schema.builder()
                .addStringField("userId")
                .addStringField("userName")
                .addRowField("game", gameSchema)
                .build();

        PCollection<Row> pCollection = input
                .setSchema(schema,
                        TypeDescriptor.of(User.class), user -> {
                            Game game = user.game;

                            Row gameRow = Row.withSchema(gameSchema)
                                    .addValues(game.userId, game.score, game.gameId, game.date)
                                    .build();

                            return Row.withSchema(schema)
                                    .addValues(user.userId, user.userName, gameRow).build();
                        },
                        row -> {
                            String userId = row.getValue("userId");
                            String userName = row.getValue("userName");
                            Row game = row.getValue("game");

                            String gameId = game.getValue("gameId");
                            Integer gameScore = game.getValue("score");
                            String gameDate = game.getValue("date");
                            return new User(userId, userName, new Game(userId, gameScore, gameId, gameDate));
                        })
                .apply(Convert.to(Row.class))
                .setCoder(RowCoder.of(schema));

        pCollection
                .apply("User", ParDo.of(new LogOutput<>("Convert to Result")));

        pipeline.run();
    }

    public static PCollection<User> getProgressPCollection(Pipeline pipeline) {
        PCollection<String> rides = pipeline
                .apply(TextIO.read().from("gs://apache-beam-samples/game/small/gaming_data.csv"));
        final PTransform<PCollection<String>, PCollection<Iterable<String>>> sample = Sample.fixedSizeGlobally(10);
        return rides.apply(sample).apply(Flatten.iterables()).apply(ParDo.of(new ExtractUserProgressFn()))
                .setCoder(CustomCoder.of());
    }

    static class ExtractUserProgressFn extends DoFn<String, User> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            String[] items = c.element().split(",");
            c.output(new User(items[0], items[1], new Game(items[0], Integer.valueOf(items[2]), items[3], items[4])));
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
                    + user.game.date;
            outStream.write(line.getBytes());
        }

        @Override
        public User decode(InputStream inStream) throws IOException {
            final String serializedDTOs = new String(StreamUtils.getBytesWithoutClosing(inStream));
            String[] params = serializedDTOs.split(";");
            String[] user = params[0].split(",");
            String[] game = params[1].split(",");
            return new User(user[0], user[1], new Game(user[0], Integer.valueOf(game[0]), game[1], game[2]));
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
