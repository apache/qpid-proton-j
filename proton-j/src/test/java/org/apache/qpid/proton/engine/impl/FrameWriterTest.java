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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.UnsignedInteger;
import org.apache.qpid.proton.amqp.security.SaslFrameBody;
import org.apache.qpid.proton.amqp.security.SaslInit;
import org.apache.qpid.proton.amqp.transport.Open;
import org.apache.qpid.proton.amqp.transport.ReceiverSettleMode;
import org.apache.qpid.proton.amqp.transport.Transfer;
import org.apache.qpid.proton.codec.AMQPDefinedTypes;
import org.apache.qpid.proton.codec.DecoderImpl;
import org.apache.qpid.proton.codec.EncoderImpl;
import org.apache.qpid.proton.codec.ReadableBuffer;
import org.apache.qpid.proton.framing.TransportFrame;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

/**
 * Tests for the FrameWriter implementation
 */
public class FrameWriterTest {

    private final TransportImpl transport = new TransportImpl();

    private final DecoderImpl decoder = new DecoderImpl();
    private final EncoderImpl encoder = new EncoderImpl(decoder);

    private ReadableBuffer bigPayload;
    private ReadableBuffer littlePayload;
    private ByteBuffer buffer;

    @Before
    public void setUp() {
        AMQPDefinedTypes.registerAllTypes(decoder, encoder);

        buffer = ByteBuffer.allocate(16384);

        encoder.setByteBuffer(buffer);
        decoder.setByteBuffer(buffer);

        Random random = new Random(System.currentTimeMillis());
        bigPayload = ReadableBuffer.ByteBufferReader.allocate(4096);
        for (int i = 0; i < bigPayload.remaining(); ++i) {
            bigPayload.array()[i] = (byte) random.nextInt(127);
        }

        littlePayload = ReadableBuffer.ByteBufferReader.allocate(16);
        for (int i = 0; i < littlePayload.remaining(); ++i) {
            littlePayload.array()[i] = (byte) random.nextInt(127);
        }
    }

    @Test
    public void testFrameWrittenToBuffer() {
        Transfer transfer = createTransfer();
        FrameWriter framer = new FrameWriter(encoder, Integer.MAX_VALUE, (byte) 0, transport);

        framer.writeFrame(transfer);
        assertNotEquals(0, framer.readBytes(buffer));

        buffer.flip();
        buffer.position(8); // Remove the Frame Header

        Object decoded = decoder.readObject();
        assertNotNull(decoded);
        assertTrue(decoded instanceof Transfer);
        Transfer result = (Transfer) decoded;

        assertEquals(UnsignedInteger.ONE, result.getHandle());
        assertEquals(UnsignedInteger.ZERO, result.getMessageFormat());
        assertEquals(UnsignedInteger.valueOf(127), result.getDeliveryId());
    }

    @Test
    public void testFailToWriteFrame() {
        FrameWriter framer = new FrameWriter(encoder, Integer.MAX_VALUE, (byte) 1, transport);

        final class FailOnUnknownType implements Function<Boolean, Boolean> {
            @Override
            public Boolean apply(Boolean t) {
                throw new IllegalStateException();
            }
        };

        Open open = new Open();
        Map<Symbol, Object> invalidProperties = new HashMap<>();
        invalidProperties.put(Symbol.valueOf("invalid-unknown-type"), new FailOnUnknownType());
        open.setProperties(invalidProperties);

        try {
            framer.writeFrame(0, open, null, null);
            fail("should have thrown exception");
        } catch (IllegalArgumentException e) {
            // Expected
            assertNotNull(e.getMessage());
            assertTrue(e.getMessage().contains(FailOnUnknownType.class.getName()));
        }

        ByteBuffer destBuffer = ByteBuffer.allocate(16);
        int read = framer.readBytes(destBuffer);

        assertEquals("should not have been any output read", 0, read);
        assertEquals("should not have been any output in buffer", 0, destBuffer.position());
    }

    @Test
    public void testFrameWrittenToBufferWithLargePayloadAndMaxFrameSizeInvokesHandlerOnce() {
        Transfer transfer = createTransfer();
        FrameWriter framer = new FrameWriter(encoder, 2048, (byte) 0, transport);

        final AtomicInteger toLargeCallbackCount = new AtomicInteger();

        framer.writeFrame(0, transfer, bigPayload, new Runnable() {

            @Override
            public void run() {
                toLargeCallbackCount.incrementAndGet();
            }
        });

        // Should have read some data during this encode.
        assertNotEquals(0, framer.readBytes(buffer));

        buffer.flip();
        byte[] header = new byte[FrameWriter.FRAME_HEADER_SIZE];
        buffer.get(header);

        ReadableBuffer headerReader = ReadableBuffer.ByteBufferReader.wrap(header);
        int size = headerReader.getInt();
        assertEquals(2048, size);

        // Should have only asked once for the handler to respond to the to large paylod.
        assertEquals(1, toLargeCallbackCount.get());
    }

    @Test
    public void testFrameWrittenToBufferWithLargePayloadAndNoMaxFrameSize() {
        Transfer transfer = createTransfer();
        FrameWriter framer = new FrameWriter(encoder, Integer.MAX_VALUE, (byte) 0, transport);

        framer.writeFrame(0, transfer, bigPayload, new PartialTransferHandler(transfer));
        assertNotEquals(0, framer.readBytes(buffer));

        buffer.flip();

        byte[] header = new byte[FrameWriter.FRAME_HEADER_SIZE];
        buffer.get(header);

        ReadableBuffer headerReader = ReadableBuffer.ByteBufferReader.wrap(header);
        int size = headerReader.getInt();
        assertTrue(size > 4096);

        Object decoded = decoder.readObject();
        assertNotNull(decoded);
        assertTrue(decoded instanceof Transfer);

        // Check for our payload
        assertTrue(buffer.hasRemaining());
        assertEquals(4096, buffer.remaining());

        byte[] payload = new byte[4096];
        buffer.get(payload);

        assertArrayEquals(bigPayload.array(), payload);
    }

    @Test
    public void testFrameWrittenToBufferWithLargePayloadAndMaxFrameSize() {
        Transfer transfer = createTransfer();
        FrameWriter framer = new FrameWriter(encoder, 2048, (byte) 0, transport);

        int payloadSize = 0;

        // Should take three write and three reads to get it all and the Transfer should
        // indicate that there is more data to come after the first two writes
        for (int i = 1; i <= 3; ++i) {
            transfer.setMore(false);
            framer.writeFrame(0, transfer, bigPayload, new PartialTransferHandler(transfer));

            ByteBuffer intermediate = ByteBuffer.allocate(4096);
            int bytesRead = framer.readBytes(intermediate);
            intermediate.flip();

            // Read the Frame Header
            byte[] header = new byte[FrameWriter.FRAME_HEADER_SIZE];
            intermediate.get(header);
            ReadableBuffer headerReader = ReadableBuffer.ByteBufferReader.wrap(header);
            int frameSize = headerReader.getInt();

            if (i < 3) {
                assertTrue(transfer.getMore());
                assertEquals(2048, bytesRead);
                assertEquals(2048, frameSize);
            } else {
                assertFalse(transfer.getMore());
                assertTrue(bytesRead < 2048);
                assertTrue(frameSize < 2048);
            }

            decoder.setBuffer(ReadableBuffer.ByteBufferReader.wrap(intermediate));
            Object decoded = decoder.readObject();
            assertNotNull(decoded);
            assertTrue(decoded instanceof Transfer);

            // Trim the Frame header and Transfer encoding size and store off actual payload
            payloadSize += bytesRead - intermediate.position();

            // Accumulate the data minus the frame headers
            buffer.put(intermediate);
        }

        assertEquals(3, framer.getFramesOutput());

        buffer.rewind();
        buffer.limit(payloadSize);

        // Check for our payload
        assertTrue(buffer.hasRemaining());
        assertEquals(4096, buffer.remaining());

        byte[] payload = new byte[4096];
        buffer.get(payload);

        assertArrayEquals(bigPayload.array(), payload);
    }

    @Test
    public void testWriteEmptyFrame() {
        FrameWriter framer = new FrameWriter(encoder, Integer.MAX_VALUE, (byte) 1, transport);

        framer.writeFrame(16, null, null, null);

        ByteBuffer headerBuffer = ByteBuffer.allocate(16);
        framer.readBytes(headerBuffer);

        assertEquals(FrameWriter.FRAME_HEADER_SIZE, headerBuffer.position());

        headerBuffer.flip();

        // Size, offset, Frame type, channel
        assertEquals(FrameWriter.FRAME_HEADER_SIZE, headerBuffer.getInt());
        assertEquals(2, headerBuffer.get());
        assertEquals(1, headerBuffer.get());
        assertEquals(16, headerBuffer.getShort());
    }

    @Test
    public void testFrameWriterReportsFullBasedOnConfiguration() {
        Transfer transfer = createTransfer();
        FrameWriter framer = new FrameWriter(encoder, Integer.MAX_VALUE, (byte) 0, transport);

        framer.writeFrame(0, transfer, bigPayload, new PartialTransferHandler(transfer));

        assertEquals(FrameWriter.DEFAULT_FRAME_BUFFER_FULL_MARK ,framer.getFrameWriterMaxBytes());
        assertFalse(framer.isFull());
        framer.setFrameWriterMaxBytes(2048);
        assertTrue(framer.isFull());
        assertEquals(2048 ,framer.getFrameWriterMaxBytes());
        framer.setFrameWriterMaxBytes(16384);
        assertFalse(framer.isFull());
        assertEquals(16384, framer.getFrameWriterMaxBytes());
    }

    @Test
    public void testFrameWriterLogsFramesToTracer() {
        List<TransportFrame> frames = new ArrayList<>();
        transport.setProtocolTracer(new ProtocolTracer()
        {
            @Override
            public void sentFrame(final TransportFrame transportFrame)
            {
                frames.add(transportFrame);
            }

            @Override
            public void receivedFrame(TransportFrame transportFrame) { }
        });

        Transfer transfer = createTransfer();
        FrameWriter framer = new FrameWriter(encoder, Integer.MAX_VALUE, (byte) 0, transport);

        framer.writeFrame(16, transfer, bigPayload, new PartialTransferHandler(transfer));

        assertEquals(1, frames.size());
        TransportFrame sentFrame = frames.get(0);

        assertEquals(16, sentFrame.getChannel());
        assertTrue(sentFrame.getBody() instanceof Transfer);

        Binary payload = sentFrame.getPayload();

        assertEquals(bigPayload.capacity(), payload.getLength());
    }

    @Test
    public void testFrameWriterLogsFramesToSystem() {
        transport.trace(2);
        TransportImpl spy = Mockito.spy(transport);

        Transfer transfer = createTransfer();
        FrameWriter framer = new FrameWriter(encoder, Integer.MAX_VALUE, (byte) 0, spy);

        int channel = 16;
        int payloadLength = littlePayload.capacity();

        framer.writeFrame(channel, transfer, littlePayload, new PartialTransferHandler(transfer));

        ArgumentCaptor<TransportFrame> frameCatcher = ArgumentCaptor.forClass(TransportFrame.class);
        Mockito.verify(spy).log(eq(TransportImpl.OUTGOING), frameCatcher.capture());

        assertEquals(channel, frameCatcher.getValue().getChannel());
        assertTrue(frameCatcher.getValue().getBody() instanceof Transfer);

        Binary payload = frameCatcher.getValue().getPayload();

        assertEquals(payloadLength, payload.getLength());
    }

    @Test
    public void testFrameWriterLogsSaslFramesToTracer() {
        List<SaslFrameBody> bodies = new ArrayList<>();
        transport.setProtocolTracer(new ProtocolTracer()
        {
            @Override
            public void sentSaslBody(final SaslFrameBody saslFrameBody)
            {
                bodies.add(saslFrameBody);
            }

            @Override
            public void receivedFrame(TransportFrame transportFrame) { }

            @Override
            public void sentFrame(TransportFrame transportFrame) { }
        });

        SaslInit init = new SaslInit();
        FrameWriter framer = new FrameWriter(encoder, Integer.MAX_VALUE,  FrameWriter.SASL_FRAME_TYPE, transport);

        framer.writeFrame(0, init, null, null);

        assertEquals(1, bodies.size());
        assertTrue(bodies.get(0) instanceof SaslInit);
    }

    @Test
    public void testFrameWriterLogsSaslFramesToSystem() {
        transport.trace(2);
        TransportImpl spy = spy(transport);

        SaslInit init = new SaslInit();
        FrameWriter framer = new FrameWriter(encoder, Integer.MAX_VALUE,  FrameWriter.SASL_FRAME_TYPE, spy);

        framer.writeFrame(0, init, null, null);

        verify(spy).log(eq(TransportImpl.OUTGOING),
                        isA(SaslInit.class));
    }

    @Test
    public void testWriteHeader() {
        FrameWriter framer = new FrameWriter(encoder, Integer.MAX_VALUE, (byte) 1, transport);
        byte[] header = new byte[] {0, 0, 0, 9, 3, 4, 0, 12};
        framer.writeHeader(header);

        ByteBuffer headerBuffer = ByteBuffer.allocate(16);
        framer.readBytes(headerBuffer);

        assertEquals(FrameWriter.FRAME_HEADER_SIZE, headerBuffer.position());

        headerBuffer.flip();

        // Size, offset, Frame type, channel
        assertEquals(FrameWriter.FRAME_HEADER_SIZE + 1, headerBuffer.getInt());
        assertEquals(3, headerBuffer.get());
        assertEquals(4, headerBuffer.get());
        assertEquals(12, headerBuffer.getShort());
    }

    private Transfer createTransfer() {
        Transfer transfer = new Transfer();
        transfer.setHandle(UnsignedInteger.ONE);
        transfer.setDeliveryTag(new Binary(new byte[] {0, 1}));
        transfer.setMessageFormat(UnsignedInteger.ZERO);
        transfer.setDeliveryId(UnsignedInteger.valueOf(127));
        transfer.setAborted(false);
        transfer.setBatchable(false);
        transfer.setRcvSettleMode(ReceiverSettleMode.SECOND);

        return transfer;
    }

    private static final class PartialTransferHandler implements Runnable {
        private Transfer transfer;

        public PartialTransferHandler(Transfer transfer) {
            this.transfer = transfer;
        }

        @Override
        public void run() {
            transfer.setMore(true);
        }
    }
}
