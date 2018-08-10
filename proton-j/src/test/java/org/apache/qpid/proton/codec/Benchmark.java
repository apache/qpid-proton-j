/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.qpid.proton.codec;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.UnsignedByte;
import org.apache.qpid.proton.amqp.UnsignedInteger;
import org.apache.qpid.proton.amqp.UnsignedShort;
import org.apache.qpid.proton.amqp.messaging.Accepted;
import org.apache.qpid.proton.amqp.messaging.ApplicationProperties;
import org.apache.qpid.proton.amqp.messaging.Data;
import org.apache.qpid.proton.amqp.messaging.Header;
import org.apache.qpid.proton.amqp.messaging.MessageAnnotations;
import org.apache.qpid.proton.amqp.messaging.Properties;
import org.apache.qpid.proton.amqp.transport.Disposition;
import org.apache.qpid.proton.amqp.transport.Flow;
import org.apache.qpid.proton.amqp.transport.Role;
import org.apache.qpid.proton.amqp.transport.Transfer;
import org.apache.qpid.proton.codec.WritableBuffer.ByteBufferWrapper;

public class Benchmark implements Runnable {

    private static final int ITERATIONS = 10 * 1024 * 1024;

    private ByteBufferWrapper outputBuf = WritableBuffer.ByteBufferWrapper.allocate(8192);
    private BenchmarkResult resultSet = new BenchmarkResult();
    private boolean warming = true;

    private final DecoderImpl decoder = new DecoderImpl();
    private final EncoderImpl encoder = new EncoderImpl(decoder);

    public static final void main(String[] args) throws IOException, InterruptedException {
        System.out.println("Current PID: " + ManagementFactory.getRuntimeMXBean().getName());
        Benchmark benchmark = new Benchmark();
        benchmark.run();
    }

    @Override
    public void run() {
        AMQPDefinedTypes.registerAllTypes(decoder, encoder);

        encoder.setByteBuffer(outputBuf);

        try {
            doBenchmarks();
            warming = false;
            doBenchmarks();
        } catch (IOException e) {
            System.out.println("Unexpected error: " + e.getMessage());
        }
    }

    private void time(String message, BenchmarkResult resultSet) {
        if (!warming) {
            System.out.println("Benchamrk of type: " + message + ": ");
            System.out.println("    Encode time = " + resultSet.getEncodeTimeMills());
            System.out.println("    Decode time = " + resultSet.getDecodeTimeMills());
        }
    }

    private final void doBenchmarks() throws IOException {
        benchmarkListOfInts();
        benchmarkUUIDs();
        benchmarkHeader();
        benchmarkProperties();
        benchmarkMessageAnnotations();
        benchmarkApplicationProperties();
        benchmarkSymbols();
        benchmarkTransfer();
        benchmarkFlow();
        benchmarkDisposition();
        benchmarkString();
        benchmarkData();
        warming = false;
    }

    private CompositeReadableBuffer convertToComposite(WritableBuffer buffer) {
        CompositeReadableBuffer composite = new CompositeReadableBuffer();
        ReadableBuffer readableView = outputBuf.toReadableBuffer();

        byte[] copy = new byte[readableView.remaining()];
        readableView.get(copy);

        return composite.append(copy);
    }

    private void benchmarkListOfInts() throws IOException {
        ArrayList<Object> list = new ArrayList<>(10);
        for (int j = 0; j < 10; j++) {
            list.add(0);
        }

        resultSet.start();
        for (int i = 0; i < ITERATIONS; i++) {
            outputBuf.byteBuffer().clear();
            encoder.writeList(list);
        }
        resultSet.encodesComplete();

        CompositeReadableBuffer inputBuf = convertToComposite(outputBuf);
        decoder.setBuffer(inputBuf);

        resultSet.start();
        for (int i = 0; i < ITERATIONS; i++) {
            decoder.readList();
            inputBuf.flip();
        }
        resultSet.decodesComplete();

        time("List<Integer>", resultSet);
    }

    private void benchmarkUUIDs() throws IOException {
        UUID uuid = UUID.randomUUID();

        resultSet.start();
        for (int i = 0; i < ITERATIONS; i++) {
            outputBuf.byteBuffer().clear();
            encoder.writeUUID(uuid);
        }
        resultSet.encodesComplete();

        CompositeReadableBuffer inputBuf = convertToComposite(outputBuf);
        decoder.setBuffer(inputBuf);

        resultSet.start();
        for (int i = 0; i < ITERATIONS; i++) {
            decoder.readUUID();
            inputBuf.flip();
        }
        resultSet.decodesComplete();

        time("UUID", resultSet);
    }

    private void benchmarkHeader() throws IOException {
        Header header = new Header();
        header.setDurable(true);
        header.setFirstAcquirer(true);

        resultSet.start();
        for (int i = 0; i < ITERATIONS; i++) {
            outputBuf.byteBuffer().clear();
            encoder.writeObject(header);
        }
        resultSet.encodesComplete();

        CompositeReadableBuffer inputBuf = convertToComposite(outputBuf);
        decoder.setBuffer(inputBuf);

        resultSet.start();
        for (int i = 0; i < ITERATIONS; i++) {
            decoder.readObject();
            inputBuf.flip();
        }
        resultSet.decodesComplete();

        time("Header", resultSet);
    }

    private void benchmarkTransfer() throws IOException {
        Transfer transfer = new Transfer();
        transfer.setDeliveryTag(new Binary(new byte[] {1, 2, 3}));
        transfer.setHandle(UnsignedInteger.valueOf(10));
        transfer.setMessageFormat(UnsignedInteger.ZERO);

        resultSet.start();
        for (int i = 0; i < ITERATIONS; i++) {
            outputBuf.byteBuffer().clear();
            encoder.writeObject(transfer);
        }
        resultSet.encodesComplete();

        CompositeReadableBuffer inputBuf = convertToComposite(outputBuf);
        decoder.setBuffer(inputBuf);

        resultSet.start();
        for (int i = 0; i < ITERATIONS; i++) {
            decoder.readObject();
            inputBuf.flip();
        }
        resultSet.decodesComplete();

        time("Transfer", resultSet);
    }

    private void benchmarkFlow() throws IOException {
        Flow flow = new Flow();
        flow.setNextIncomingId(UnsignedInteger.valueOf(1));
        flow.setIncomingWindow(UnsignedInteger.valueOf(2047));
        flow.setNextOutgoingId(UnsignedInteger.valueOf(1));
        flow.setOutgoingWindow(UnsignedInteger.MAX_VALUE);
        flow.setHandle(UnsignedInteger.ZERO);
        flow.setDeliveryCount(UnsignedInteger.valueOf(10));
        flow.setLinkCredit(UnsignedInteger.valueOf(1000));

        resultSet.start();
        for (int i = 0; i < ITERATIONS; i++) {
            outputBuf.byteBuffer().clear();
            encoder.writeObject(flow);
        }
        resultSet.encodesComplete();

        CompositeReadableBuffer inputBuf = convertToComposite(outputBuf);
        decoder.setBuffer(inputBuf);

        resultSet.start();
        for (int i = 0; i < ITERATIONS; i++) {
            decoder.readObject();
            inputBuf.flip();
        }
        resultSet.decodesComplete();

        time("Flow", resultSet);
    }

    private void benchmarkProperties() throws IOException {
        Properties properties = new Properties();
        properties.setTo("queue:1-1024");
        properties.setReplyTo("queue:1-11024-reply");
        properties.setMessageId("ID:255f1297-5a71-4df1-8147-b2cdf850a56f:1");
        properties.setCreationTime(new Date(System.currentTimeMillis()));

        resultSet.start();
        for (int i = 0; i < ITERATIONS; i++) {
            outputBuf.byteBuffer().clear();
            encoder.writeObject(properties);
        }
        resultSet.encodesComplete();

        CompositeReadableBuffer inputBuf = convertToComposite(outputBuf);
        decoder.setBuffer(inputBuf);

        resultSet.start();
        for (int i = 0; i < ITERATIONS; i++) {
            decoder.readObject();
            inputBuf.flip();
        }
        resultSet.decodesComplete();

        time("Properties", resultSet);
    }

    private void benchmarkMessageAnnotations() throws IOException {
        MessageAnnotations annotations = new MessageAnnotations(new HashMap<Symbol, Object>());
        annotations.getValue().put(Symbol.valueOf("test1"), UnsignedByte.valueOf((byte) 128));
        annotations.getValue().put(Symbol.valueOf("test2"), UnsignedShort.valueOf((short) 128));
        annotations.getValue().put(Symbol.valueOf("test3"), UnsignedInteger.valueOf((byte) 128));

        resultSet.start();
        for (int i = 0; i < ITERATIONS; i++) {
            outputBuf.byteBuffer().clear();
            encoder.writeObject(annotations);
        }
        resultSet.encodesComplete();

        CompositeReadableBuffer inputBuf = convertToComposite(outputBuf);
        decoder.setBuffer(inputBuf);

        resultSet.start();
        for (int i = 0; i < ITERATIONS; i++) {
            decoder.readObject();
            inputBuf.flip();
        }
        resultSet.decodesComplete();

        time("MessageAnnotations", resultSet);
    }

    private void benchmarkApplicationProperties() throws IOException {
        ApplicationProperties properties = new ApplicationProperties(new HashMap<String, Object>());
        properties.getValue().put("test1", UnsignedByte.valueOf((byte) 128));
        properties.getValue().put("test2", UnsignedShort.valueOf((short) 128));
        properties.getValue().put("test3", UnsignedInteger.valueOf((byte) 128));

        resultSet.start();
        for (int i = 0; i < ITERATIONS; i++) {
            outputBuf.byteBuffer().clear();
            encoder.writeObject(properties);
        }
        resultSet.encodesComplete();

        CompositeReadableBuffer inputBuf = convertToComposite(outputBuf);
        decoder.setBuffer(inputBuf);

        resultSet.start();
        for (int i = 0; i < ITERATIONS; i++) {
            decoder.readObject();
            inputBuf.flip();
        }
        resultSet.decodesComplete();

        time("ApplicationProperties", resultSet);
    }

    private void benchmarkSymbols() throws IOException {
        Symbol symbol1 = Symbol.valueOf("Symbol-1");
        Symbol symbol2 = Symbol.valueOf("Symbol-2");
        Symbol symbol3 = Symbol.valueOf("Symbol-3");

        resultSet.start();
        for (int i = 0; i < ITERATIONS; i++) {
            outputBuf.byteBuffer().clear();
            encoder.writeSymbol(symbol1);
            encoder.writeSymbol(symbol2);
            encoder.writeSymbol(symbol3);
        }
        resultSet.encodesComplete();

        CompositeReadableBuffer inputBuf = convertToComposite(outputBuf);
        decoder.setBuffer(inputBuf);

        resultSet.start();
        for (int i = 0; i < ITERATIONS; i++) {
            decoder.readSymbol();
            decoder.readSymbol();
            decoder.readSymbol();
            inputBuf.flip();
        }
        resultSet.decodesComplete();

        time("Symbol", resultSet);
    }

    private void benchmarkDisposition() throws IOException {
        Disposition disposition = new Disposition();
        disposition.setRole(Role.RECEIVER);
        disposition.setSettled(true);
        disposition.setState(Accepted.getInstance());
        disposition.setFirst(UnsignedInteger.valueOf(2));
        disposition.setLast(UnsignedInteger.valueOf(2));

        resultSet.start();
        for (int i = 0; i < ITERATIONS; i++) {
            outputBuf.byteBuffer().clear();
            encoder.writeObject(disposition);
        }
        resultSet.encodesComplete();

        CompositeReadableBuffer inputBuf = convertToComposite(outputBuf);
        decoder.setBuffer(inputBuf);

        resultSet.start();
        for (int i = 0; i < ITERATIONS; i++) {
            decoder.readObject();
            inputBuf.flip();
        }
        resultSet.decodesComplete();

        time("Disposition", resultSet);
    }

    private void benchmarkString() throws IOException {
        String string1 = new String("String-1-somewhat-long-test-to-validate-performance-improvements-to-the-proton-j-codec-@!%$");
        String string2 = new String("String-2-somewhat-long-test-to-validate-performance-improvements-to-the-proton-j-codec-@!%$");
        String string3 = new String("String-3-somewhat-long-test-to-validate-performance-improvements-to-the-proton-j-codec-@!%$");

        resultSet.start();
        for (int i = 0; i < ITERATIONS; i++) {
            outputBuf.byteBuffer().clear();
            encoder.writeString(string1);
            encoder.writeString(string2);
            encoder.writeString(string3);
        }
        resultSet.encodesComplete();

        CompositeReadableBuffer inputBuf = convertToComposite(outputBuf);
        decoder.setBuffer(inputBuf);

        resultSet.start();
        for (int i = 0; i < ITERATIONS; i++) {
            decoder.readString();
            decoder.readString();
            decoder.readString();
            inputBuf.flip();
        }
        resultSet.decodesComplete();

        time("String", resultSet);
    }

    private void benchmarkData() throws IOException {
        Data data1 = new Data(new Binary(new byte[] {1, 2, 3}));
        Data data2 = new Data(new Binary(new byte[] {4, 5, 6}));
        Data data3 = new Data(new Binary(new byte[] {7, 8, 9}));

        resultSet.start();
        for (int i = 0; i < ITERATIONS; i++) {
            outputBuf.byteBuffer().clear();
            encoder.writeObject(data1);
            encoder.writeObject(data2);
            encoder.writeObject(data3);
        }
        resultSet.encodesComplete();

        CompositeReadableBuffer inputBuf = convertToComposite(outputBuf);
        decoder.setBuffer(inputBuf);

        resultSet.start();
        for (int i = 0; i < ITERATIONS; i++) {
            decoder.readObject();
            decoder.readObject();
            decoder.readObject();
            inputBuf.flip();
        }
        resultSet.decodesComplete();

        time("Data", resultSet);
    }

    private static class BenchmarkResult {

        private long startTime;

        private long encodeTime;
        private long decodeTime;

        public void start() {
            startTime = System.nanoTime();
        }

        public void encodesComplete() {
            encodeTime = System.nanoTime() - startTime;
        }

        public void decodesComplete() {
            decodeTime = System.nanoTime() - startTime;
        }

        public long getEncodeTimeMills() {
            return TimeUnit.NANOSECONDS.toMillis(encodeTime);
        }

        public long getDecodeTimeMills() {
            return TimeUnit.NANOSECONDS.toMillis(decodeTime);
        }
    }
}
