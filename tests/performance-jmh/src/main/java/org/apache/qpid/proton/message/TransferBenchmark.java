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
import org.apache.qpid.proton.amqp.UnsignedInteger;
import org.apache.qpid.proton.amqp.transport.Transfer;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.runner.RunnerException;

import java.nio.ByteBuffer;

public class TransferBenchmark extends MessageBenchmark
{

    private Transfer transfer;

    @Setup
    public void init()
    {
        super.init();
        initTransfer();
        encode();
    }

    private void initTransfer()
    {
        transfer = new Transfer();
        transfer.setDeliveryTag(new Binary(new byte[] {1, 2, 3}));
        transfer.setHandle(UnsignedInteger.valueOf(10));
        transfer.setMessageFormat(UnsignedInteger.ZERO);
    }

    @Benchmark
    @Override
    public Object decode()
    {
        return super.decode();
    }

    @Benchmark
    @Override
    public ByteBuffer encode()
    {
        return encodeObj(transfer);
    }

    public static void main(String[] args) throws RunnerException
    {
        runBenchmark(TransferBenchmark.class);
    }
}
