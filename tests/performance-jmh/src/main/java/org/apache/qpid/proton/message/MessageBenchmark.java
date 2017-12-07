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

package org.apache.qpid.proton.message;

import org.apache.qpid.proton.codec.AMQPDefinedTypes;
import org.apache.qpid.proton.codec.DecoderImpl;
import org.apache.qpid.proton.codec.EncoderImpl;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.profile.GCProfiler;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 5, time = 1)
public abstract class MessageBenchmark
{

    public static final int DEFAULT_BUFFER_SIZE = 8192;
    protected ByteBuffer byteBuf;
    protected DecoderImpl decoder;
    protected EncoderImpl encoder;

    /**
     * It could be overridden to allow encoding/decoding buffer to be sized differently from  {@link #DEFAULT_BUFFER_SIZE}
     */
    protected int bufferSize()
    {
        return DEFAULT_BUFFER_SIZE;
    }

    public void init()
    {
        byteBuf = ByteBuffer.allocate(bufferSize());
        this.decoder = new DecoderImpl();
        this.encoder = new EncoderImpl(decoder);
        AMQPDefinedTypes.registerAllTypes(decoder, encoder);
        //initialize encoders
        encoder.setByteBuffer(byteBuf);
        decoder.setByteBuffer(byteBuf);
    }

    public abstract ByteBuffer encode();

    protected final ByteBuffer encodeObj(Object obj)
    {
        byteBuf.clear();
        encoder.writeObject(obj);
        return byteBuf;
    }

    /**
     * By default it performs a {@link DecoderImpl#readObject()}.
     */
    protected Object decode()
    {
        byteBuf.flip();
        return decoder.readObject();
    }

    public static void main(String[] args) throws RunnerException
    {
        runBenchmark(MessageBenchmark.class);
    }

    public static void runBenchmark(Class<?> benchmarkClass) throws RunnerException
    {
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
