/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.qpid.proton.codec;

import org.apache.qpid.proton.codec.ReadableBuffer.ByteBufferReader;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.profile.GCProfiler;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

/**
 * Test performance of the CompositeReadableBuffer class
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 5, time = 1)
public class CompositeReadableBufferBenchmark {

    private CompositeReadableBuffer composite;
    @Param({"8", "64", "1024"})
    private int size;
    private ReadableBuffer.ByteBufferReader bufferReader;
    @Param({"false", "true"})
    private boolean direct;
    @Param({"1", "2"})
    private int chunks;

    @Setup
    public void init() {
        bufferReader =
                direct ? ByteBufferReader.wrap(ByteBuffer.allocateDirect(size)) : ByteBufferReader.wrap(new byte[size]);
        composite = new CompositeReadableBuffer();
        int sizePerChunk = size / chunks;
        for (int i = 0; i < chunks; i++) {
            final byte[] content = new byte[sizePerChunk];
            composite.append(content);
        }
        int remaining = size - composite.capacity();
        if (remaining > 0) {
            byte[] lastChunk = new byte[remaining];
            composite.append(lastChunk);
        }
    }

    @Benchmark
    public boolean equalsToByteBufferReader() {
        composite.position(0);
        return composite.equals(bufferReader);
    }

    public static void main(String[] args) throws RunnerException {
        runBenchmark(CompositeReadableBuffer.class);
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
