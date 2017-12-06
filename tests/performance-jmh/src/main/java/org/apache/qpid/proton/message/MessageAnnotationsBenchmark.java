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
import org.apache.qpid.proton.amqp.UnsignedByte;
import org.apache.qpid.proton.amqp.UnsignedInteger;
import org.apache.qpid.proton.amqp.UnsignedShort;
import org.apache.qpid.proton.amqp.messaging.MessageAnnotations;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.runner.RunnerException;

import java.nio.ByteBuffer;
import java.util.HashMap;

public class MessageAnnotationsBenchmark extends MessageBenchmark
{

    private MessageAnnotations annotations;

    @Setup
    public void init()
    {
        super.init();
        initMessageAnnotations();
        encode();
    }

    private void initMessageAnnotations()
    {
        annotations = new MessageAnnotations(new HashMap<Symbol, Object>());
        annotations.getValue().put(Symbol.valueOf("test1"), UnsignedByte.valueOf((byte) 128));
        annotations.getValue().put(Symbol.valueOf("test2"), UnsignedShort.valueOf((short) 128));
        annotations.getValue().put(Symbol.valueOf("test3"), UnsignedInteger.valueOf((byte) 128));
    }

    @Benchmark
    @Override
    public ByteBuffer encode()
    {
        return super.encodeObj(annotations);
    }

    @Benchmark
    @Override
    public Object decode()
    {
        return super.decode();
    }

    public static void main(String[] args) throws RunnerException
    {
        runBenchmark(MessageAnnotationsBenchmark.class);
    }
}
