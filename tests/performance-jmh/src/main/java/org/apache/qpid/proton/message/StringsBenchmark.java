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

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.RunnerException;

import java.nio.ByteBuffer;

public class StringsBenchmark extends MessageBenchmark
{

    private Blackhole blackhole;
    private String string1;
    private String string2;
    private String string3;

    @Setup
    public void init(Blackhole blackhole)
    {
        this.blackhole = blackhole;
        super.init();
        initStrings();
        encode();
    }

    private void initStrings()
    {
        string1 = new String("String-1");
        string2 = new String("String-2");
        string3 = new String("String-3");
    }

    @Benchmark
    @Override
    public ByteBuffer encode()
    {
        byteBuf.clear();
        encoder.writeString(string1);
        encoder.writeString(string2);
        encoder.writeString(string3);
        return byteBuf;
    }

    @Benchmark
    @Override
    public Object decode()
    {
        byteBuf.flip();
        blackhole.consume(decoder.readString());
        blackhole.consume(decoder.readString());
        blackhole.consume(decoder.readString());
        return byteBuf;
    }

    public static void main(String[] args) throws RunnerException
    {
        runBenchmark(StringsBenchmark.class);
    }
}
