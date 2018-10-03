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
package org.apache.qpid.proton.engine.impl;

import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

import org.apache.qpid.proton.Proton;
import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.UnsignedInteger;
import org.apache.qpid.proton.amqp.transport.Transfer;
import org.apache.qpid.proton.codec.AMQPDefinedTypes;
import org.apache.qpid.proton.codec.DecoderImpl;
import org.apache.qpid.proton.codec.EncoderImpl;
import org.apache.qpid.proton.codec.ReadableBuffer;
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

/**
 * Test performance of the FrameWriter class
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 5, time = 1)
public class FrameWriterBenchmark {

    private static final byte[] PAYLOAD_BYTES = {
        0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15,
        0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15,
        0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15,
        0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15,
        0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15,
        0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15,
        0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15,
        0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15
    };

    public static final int DEFAULT_BUFFER_SIZE = 8192;

    private TransportImpl transport;
    private FrameWriter frameWriter;
    private ByteBuffer byteBuf;
    private DecoderImpl decoder;
    private EncoderImpl encoder;

    private Transfer transfer;
    private ReadableBuffer payload;

    @Setup
    public void init(Blackhole blackhole)
    {
        initProton();
    }

    public void initProton() {
        byteBuf = ByteBuffer.allocate(DEFAULT_BUFFER_SIZE);
        this.decoder = new DecoderImpl();
        this.encoder = new EncoderImpl(decoder);
        AMQPDefinedTypes.registerAllTypes(decoder, encoder);

        transport = (TransportImpl) Proton.transport();
        frameWriter = new FrameWriter(encoder, 16 * 1024, (byte) 0, transport);

        transfer = new Transfer();
        transfer.setDeliveryId(UnsignedInteger.ONE);
        transfer.setHandle(UnsignedInteger.valueOf(16));
        transfer.setDeliveryTag(new Binary(new byte[] { 0, 1}));
        transfer.setMessageFormat(UnsignedInteger.ZERO);

        payload = ReadableBuffer.ByteBufferReader.wrap(PAYLOAD_BYTES);
    }

    @Benchmark
    public ByteBuffer writeTransferPerformative()
    {
        byteBuf.clear();

        frameWriter.writeFrame(0, transfer, payload, null);
        frameWriter.readBytes(byteBuf);

        return byteBuf;
    }

    public static void main(String[] args) throws RunnerException
    {
        runBenchmark(FrameWriterBenchmark.class);
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
