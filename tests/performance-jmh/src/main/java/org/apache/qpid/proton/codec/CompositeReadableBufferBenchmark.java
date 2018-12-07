/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.qpid.proton.codec;

import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.profile.GCProfiler;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 5, time = 1)
public class CompositeReadableBufferBenchmark {

    private static final String PAYLOAD =
        "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789"
      + "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789"
      + "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789"
      + "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789"
      + "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789";

    private static final String PAYLOAD_CHUNK =
        "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789";

    private final CharsetDecoder decoder = StandardCharsets.UTF_8.newDecoder();

    private CompositeReadableBuffer singleArrayString;
    private CompositeReadableBuffer splitArrayString;

    private Blackhole blackhole;

    @Setup
    public void init(Blackhole blackhole) {
        this.blackhole = blackhole;

        byte[] payload = PAYLOAD.getBytes(StandardCharsets.UTF_8);

        singleArrayString = new CompositeReadableBuffer();
        singleArrayString.append(Arrays.copyOf(payload, payload.length));

        byte[] payloadChunk = PAYLOAD_CHUNK.getBytes(StandardCharsets.UTF_8);

        splitArrayString = new CompositeReadableBuffer();

        for (int i = 0; i < 5; i++) {
            splitArrayString.append(Arrays.copyOf(payloadChunk, payloadChunk.length));
        }
    }

    @Benchmark
    public void hashCodeSingleArrayFullSpan() {
        blackhole.consume(singleArrayString.rewind().hashCode());
    }

    @Benchmark
    public void hashCodeMultipleArrayFullSpan() throws CharacterCodingException {
        blackhole.consume(splitArrayString.rewind().hashCode());
    }

    @Benchmark
    public void hashCodeSingArraySpanInSingleArray() throws CharacterCodingException {
        blackhole.consume(singleArrayString.rewind().limit(PAYLOAD_CHUNK.length()).hashCode());
    }

    @Benchmark
    public void hashCodeMultipleArraySpanInSingleArray() throws CharacterCodingException {
        blackhole.consume(splitArrayString.rewind().limit(PAYLOAD_CHUNK.length()).hashCode());
    }

    @Benchmark
    public void readStringSingleArray() throws CharacterCodingException {
        blackhole.consume(singleArrayString.rewind().readString(decoder.reset()));
    }

    @Benchmark
    public void readStringMultiArray() throws CharacterCodingException {
        blackhole.consume(splitArrayString.rewind().readString(decoder.reset()));
    }

    @Benchmark
    public void sliceSingleArrayFullSpan() {
        blackhole.consume(singleArrayString.rewind().slice());
    }

    @Benchmark
    public void sliceMultipleArrayFullSpan() {
        blackhole.consume(splitArrayString.rewind().slice());
    }

    @Benchmark
    public void duplicateSingleArrayFullSpan() {
        blackhole.consume(singleArrayString.rewind().duplicate());
    }

    @Benchmark
    public void duplicateMultipleArrayFullSpan() {
        blackhole.consume(splitArrayString.rewind().duplicate());
    }

    @Benchmark
    public void getFromSingleArray() {
        blackhole.consume(singleArrayString.rewind().get());
    }

    @Benchmark
    public void getFromMultipleArray() {
        blackhole.consume(splitArrayString.rewind().get());
    }

    @Benchmark
    public void getShortFromSingleArray() {
        blackhole.consume(singleArrayString.rewind().getShort());
    }

    @Benchmark
    public void getShortFromMultipleArray() {
        blackhole.consume(splitArrayString.rewind().getShort());
    }

    @Benchmark
    public void getIntFromSingleArray() {
        blackhole.consume(singleArrayString.rewind().getInt());
    }

    @Benchmark
    public void getIntFromMultipleArray() {
        blackhole.consume(splitArrayString.rewind().getInt());
    }

    @Benchmark
    public void getLongFromSingleArray() {
        blackhole.consume(singleArrayString.rewind().getLong());
    }

    @Benchmark
    public void getLongFromMultipleArray() {
        blackhole.consume(splitArrayString.rewind().getLong());
    }

    @Benchmark
    public void positionSingleArrayHalfPlusOne() {
        blackhole.consume(singleArrayString.rewind().position((singleArrayString.remaining() / 2) + 1));
    }

    @Benchmark
    public void positionMultiArrayHalfPlusOne() {
        blackhole.consume(splitArrayString.rewind().position((splitArrayString.remaining() / 2) + 1));
    }

    public static void main(String[] args) throws RunnerException {
        runBenchmark(CompositeReadableBufferBenchmark.class);
    }

    public static void runBenchmark(Class<?> benchmarkClass) throws RunnerException {
        final Options opt = new OptionsBuilder()
            .include(benchmarkClass.getSimpleName())
            .addProfiler(GCProfiler.class)
            .shouldDoGC(true)
            .warmupIterations(5)
            .measurementIterations(5)
            .forks(1)
            .build();
        new Runner(opt).run();
    }
}
