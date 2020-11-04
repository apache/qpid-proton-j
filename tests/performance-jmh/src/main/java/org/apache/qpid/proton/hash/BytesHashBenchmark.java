/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.apache.qpid.proton.hash;

import org.apache.qpid.proton.codec.Hashing;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.nio.ByteBuffer;
import java.util.SplittableRandom;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(2)
public class BytesHashBenchmark {

    @Param({ "7", "24", "32" })
    int size;

    @Param({ "6", "12" })
    int logPermutations;

    @Param({ "1" })
    int seed;

    int permutations;

    ByteBuffer[] data;
    private int i;

    @Param({ "false", "true" })
    private boolean direct;

    @Setup(Level.Trial)
    public void init() {
        SplittableRandom random = new SplittableRandom(seed);
        permutations = 1 << logPermutations;
        this.data = new ByteBuffer[permutations];
        for (int i = 0; i < permutations; ++i) {
            data[i] = direct? ByteBuffer.allocateDirect(size) : ByteBuffer.allocate(size);
            final int limit = random.nextInt(Math.max(0, size - 8), size);
            for (int j = 0; j < limit; j++) {
                int value = random.nextInt(Byte.MIN_VALUE, Byte.MAX_VALUE + 1);
                data[i].put(j, (byte) value);
            }
            data[i].limit(limit);
        }
    }

    @Benchmark
    public int vanillaHashCode() {
        return getData().hashCode();
    }

    @Benchmark
    public int protonHashCode() {
        return Hashing.byteBufferCompatibleHashCode(getData());
    }

    private ByteBuffer getData() {
        return data[i++ & (permutations - 1)];
    }

    public static void main(String[] args) throws RunnerException {
        runBenchmark(BytesHashBenchmark.class);
    }

    public static void runBenchmark(Class<?> benchmarkClass) throws RunnerException {
        final Options opt = new OptionsBuilder()
                .include(benchmarkClass.getSimpleName())
                .build();
        new Runner(opt).run();
    }
}
