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
import org.openjdk.jmh.runner.RunnerException;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class ListOfIntsBenchmark extends MessageBenchmark
{

    private static final int LIST_SIZE = 10;
    private ArrayList<Object> listOfInts;

    @Setup
    public void init()
    {
        super.init();
        initListOfInts();
        encode();
    }

    private void initListOfInts()
    {
        this.listOfInts = new ArrayList<>(LIST_SIZE);
        for (int i = 0; i < LIST_SIZE; i++)
        {
            listOfInts.add(i);
        }
    }

    @Benchmark
    @Override
    public ByteBuffer encode()
    {
        byteBuf.clear();
        encoder.writeList(listOfInts);
        return byteBuf;
    }

    @Benchmark
    @Override
    public List decode()
    {
        byteBuf.flip();
        return decoder.readList();
    }

    public static void main(String[] args) throws RunnerException
    {
        runBenchmark(ListOfIntsBenchmark.class);
    }

}