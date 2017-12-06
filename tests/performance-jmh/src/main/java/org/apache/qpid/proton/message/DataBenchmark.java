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

import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.messaging.Data;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.RunnerException;

import java.nio.ByteBuffer;

public class DataBenchmark extends MessageBenchmark
{

    private Blackhole blackhole;
    private Data data1;
    private Data data2;
    private Data data3;

    @Setup
    public void init(Blackhole blackhole)
    {
        this.blackhole = blackhole;
        super.init();
        initData();
        encode();
    }

    private void initData()
    {
        data1 = new Data(new Binary(new byte[] {1, 2, 3}));
        data2 = new Data(new Binary(new byte[] {4, 5, 6}));
        data3 = new Data(new Binary(new byte[] {7, 8, 9}));
    }

    @Benchmark
    @Override
    public ByteBuffer encode()
    {
        byteBuf.clear();
        encoder.writeObject(data1);
        encoder.writeObject(data2);
        encoder.writeObject(data3);
        return byteBuf;
    }

    @Benchmark
    @Override
    public Object decode()
    {
        byteBuf.flip();
        blackhole.consume(decoder.readObject());
        blackhole.consume(decoder.readObject());
        blackhole.consume(decoder.readObject());
        return byteBuf;
    }

    public static void main(String[] args) throws RunnerException
    {
        runBenchmark(DataBenchmark.class);
    }
}
