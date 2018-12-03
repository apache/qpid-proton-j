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
public class CompositeReadableBufferStringDecodeBenchmark {

    private static final String PAYLOAD =
          "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789"
        + "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789"
        + "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789"
        + "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789"
        + "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789";

    private static final String PAYLOAD_CHUNK =
        "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789";

    private final CharsetDecoder charsetDecoder = StandardCharsets.UTF_8.newDecoder();

    private CompositeReadableBuffer singleArrayString;
    private CompositeReadableBuffer splitArrayString;

    private Blackhole blackhole;

    @Setup
    public void init(Blackhole blackhole) {
        this.blackhole = blackhole;

        byte[] payload = PAYLOAD.getBytes(StandardCharsets.UTF_8);
        singleArrayString = new CompositeReadableBuffer();
        singleArrayString.append(payload);

        splitArrayString = new CompositeReadableBuffer();

        byte[] payloadChunk = PAYLOAD_CHUNK.getBytes(StandardCharsets.UTF_8);
        for (int i = 0; i < 5; i++) {
            splitArrayString.append(Arrays.copyOf(payloadChunk, payloadChunk.length));
        }
    }

    @Benchmark
    public void decodeSingleArray() throws CharacterCodingException {
        charsetDecoder.reset();
        blackhole.consume(singleArrayString.readString(charsetDecoder));
        singleArrayString.rewind();
    }

    @Benchmark
    public void decodeMultiArray() throws CharacterCodingException {
        charsetDecoder.reset();
        blackhole.consume(splitArrayString.readString(charsetDecoder));
        singleArrayString.rewind();
    }

    public static void main(String[] args) throws RunnerException {
        runBenchmark(CompositeReadableBufferStringDecodeBenchmark.class);
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
