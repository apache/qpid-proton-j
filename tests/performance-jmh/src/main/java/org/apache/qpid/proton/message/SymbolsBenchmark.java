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

import org.apache.qpid.proton.amqp.Symbol;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.RunnerException;

import java.nio.ByteBuffer;

public class SymbolsBenchmark extends MessageBenchmark
{

    private Symbol symbol1;
    private Symbol symbol2;
    private Symbol symbol3;
    private Blackhole blackhole;

    @Setup
    public void init(Blackhole blackhole)
    {
        this.blackhole = blackhole;
        super.init();
        initSymbols();
        encode();
    }

    private void initSymbols()
    {
        symbol1 = Symbol.valueOf("Symbol-1");
        symbol2 = Symbol.valueOf("Symbol-2");
        symbol3 = Symbol.valueOf("Symbol-3");
    }

    @Benchmark
    @Override
    public ByteBuffer encode()
    {
        byteBuf.clear();
        encoder.writeSymbol(symbol1);
        encoder.writeSymbol(symbol2);
        encoder.writeSymbol(symbol3);
        return byteBuf;
    }

    @Benchmark
    @Override
    public Object decode()
    {
        byteBuf.flip();
        //these ones are necessary to avoid JVM erase the decode processing/symbol allocations
        blackhole.consume(decoder.readSymbol());
        blackhole.consume(decoder.readSymbol());
        blackhole.consume(decoder.readSymbol());
        return byteBuf;
    }

    public static void main(String[] args) throws RunnerException
    {
        runBenchmark(SymbolsBenchmark.class);
    }
}