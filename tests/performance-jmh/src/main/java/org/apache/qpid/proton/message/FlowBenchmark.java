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

import org.apache.qpid.proton.amqp.UnsignedInteger;
import org.apache.qpid.proton.amqp.transport.Flow;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.runner.RunnerException;

import java.nio.ByteBuffer;

public class FlowBenchmark extends MessageBenchmark
{

    private Flow flow;

    @Setup
    public void init()
    {
        super.init();
        initFlow();
        encode();
    }

    private void initFlow()
    {
        flow = new Flow();
        flow.setNextIncomingId(UnsignedInteger.valueOf(1));
        flow.setIncomingWindow(UnsignedInteger.valueOf(2047));
        flow.setNextOutgoingId(UnsignedInteger.valueOf(1));
        flow.setOutgoingWindow(UnsignedInteger.MAX_VALUE);
        flow.setHandle(UnsignedInteger.ZERO);
        flow.setDeliveryCount(UnsignedInteger.valueOf(10));
        flow.setLinkCredit(UnsignedInteger.valueOf(1000));
    }

    @Benchmark
    @Override
    public ByteBuffer encode()
    {
        return encodeObj(flow);
    }

    @Benchmark
    @Override
    public Object decode()
    {
        return super.decode();
    }

    public static void main(String[] args) throws RunnerException
    {
        runBenchmark(FlowBenchmark.class);
    }
}
