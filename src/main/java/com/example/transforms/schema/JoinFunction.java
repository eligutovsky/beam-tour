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

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.JavaFieldSchema;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.annotations.SchemaCreate;
import org.apache.beam.sdk.schemas.transforms.Join;
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

public class JoinFunction {
    private static final Logger LOG = LoggerFactory.getLogger(JoinFunction.class);

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
            return "Game{" + "userId='" + userId + '\'' + ", score='" + score + '\'' + ", gameId='" + gameId + '\''
                    + ", date='" + date + '\'' + '}';
        }
    }

    // User schema
    @DefaultSchema(JavaFieldSchema.class)
    public static class User {
        public String userId;
        public String userName;

        @SchemaCreate
        public User(String userId, String userName) {
            this.userId = userId;
            this.userName = userName;
        }

        @Override
        public String toString() {
            return "User{" + "userId='" + userId + '\'' + ", userName='" + userName + '\'' + '}';
        }
    }

    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
        Pipeline pipeline = Pipeline.create(options);

        Schema userSchema = Schema.builder()
                .addStringField("userId")
                .addStringField("userName")
                .build();

        Schema gameSchema = Schema.builder()
                .addStringField("userId")
                .addInt32Field("score")
                .addStringField("gameId")
                .addStringField("date")
                .build();

        PCollection<User> userInfo = getUserPCollection(pipeline).setSchema(userSchema, TypeDescriptor.of(User.class),
                input -> {
                    User user = input;
                    return Row.withSchema(userSchema).addValues(user.userId, user.userName).build();
                }, input -> new User(input.getString(0), input.getString(1)));

        PCollection<Game> gameInfo = getGamePCollection(pipeline).setSchema(gameSchema, TypeDescriptor.of(Game.class),
                row -> {
                    Game game = row;
                    return Row.withSchema(gameSchema).addValues(game.userId, game.score, game.gameId, game.date)
                            .build();
                }, row -> new Game(row.getString(0), row.getInt32(1), row.getString(2), row.getString(3)));

        PCollection<Row> pCollection = userInfo.apply(Join.<User, Game>fullOuterJoin(gameInfo).using("userId"));

        pCollection.apply("User flatten row", ParDo.of(new LogOutput<>("Flattened")));

        pipeline.run();
    }

    static class UserCoder extends Coder<JoinFunction.User> {
        private static final UserCoder INSTANCE = new UserCoder();

        public static UserCoder of() {
            return INSTANCE;
        }

        @Override
        public void encode(JoinFunction.User user, OutputStream outStream) throws IOException {
            String line = user.userId + "," + user.userName;
            outStream.write(line.getBytes());
        }

        @Override
        public JoinFunction.User decode(InputStream inStream) throws IOException {
            final String serializedDTOs = new String(StreamUtils.getBytesWithoutClosing(inStream));
            String[] params = serializedDTOs.split(",");
            return new JoinFunction.User(params[0], params[1]);
        }

        @Override
        public List<? extends Coder<?>> getCoderArguments() {
            return Collections.emptyList();
        }

        @Override
        public void verifyDeterministic() {
        }
    }

    static class GameCoder extends Coder<JoinFunction.Game> {
        private static final GameCoder INSTANCE = new GameCoder();

        public static GameCoder of() {
            return INSTANCE;
        }

        @Override
        public void encode(JoinFunction.Game game, OutputStream outStream) throws IOException {
            String line = game.userId + "," + game.score + "," + game.gameId + "," + game.date;
            outStream.write(line.getBytes());
        }

        @Override
        public JoinFunction.Game decode(InputStream inStream) throws IOException {
            final String serializedDTOs = new String(StreamUtils.getBytesWithoutClosing(inStream));
            String[] params = serializedDTOs.split(",");
            return new JoinFunction.Game(params[0], Integer.valueOf(params[2]), params[3], params[4]);
        }

        @Override
        public List<? extends Coder<?>> getCoderArguments() {
            return Collections.emptyList();
        }

        @Override
        public void verifyDeterministic() {
        }
    }

    public static PCollection<User> getUserPCollection(Pipeline pipeline) {
        PCollection<String> rides = pipeline
                .apply(TextIO.read().from("gs://apache-beam-samples/game/small/gaming_data.csv"));
        final PTransform<PCollection<String>, PCollection<Iterable<String>>> sample = Sample.fixedSizeGlobally(100);
        return rides.apply(sample).apply(Flatten.iterables()).apply(ParDo.of(new ExtractUserFn()))
                .setCoder(UserCoder.of());
    }

    public static PCollection<Game> getGamePCollection(Pipeline pipeline) {
        PCollection<String> rides = pipeline
                .apply(TextIO.read().from("gs://apache-beam-samples/game/small/gaming_data.csv"));
        final PTransform<PCollection<String>, PCollection<Iterable<String>>> sample = Sample.fixedSizeGlobally(100);
        return rides.apply(sample).apply(Flatten.iterables()).apply(ParDo.of(new ExtractUserProgressFn()))
                .setCoder(GameCoder.of());
    }

    static class ExtractUserFn extends DoFn<String, User> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            String[] items = c.element().split(",");
            c.output(new User(items[0], items[1]));
        }
    }

    static class ExtractUserProgressFn extends DoFn<String, Game> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            String[] items = c.element().split(",");
            c.output(new Game(items[0], Integer.valueOf(items[2]), items[3], items[4]));
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
