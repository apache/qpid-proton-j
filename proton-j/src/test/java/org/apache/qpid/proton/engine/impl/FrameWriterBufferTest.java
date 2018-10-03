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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.nio.ByteBuffer;

import org.apache.qpid.proton.codec.ReadableBuffer;
import org.junit.Test;

/**
 * Test behavior of the FrameWriterBuffer implementation
 */
public class FrameWriterBufferTest {

    //----- Test newly create buffer behaviors -------------------------------//

    @Test
    public void testDefaultCtor() {
        FrameWriterBuffer buffer = new FrameWriterBuffer();

        assertEquals(FrameWriterBuffer.DEFAULT_CAPACITY, buffer.array().length);
        assertEquals(0, buffer.arrayOffset());
        assertEquals(Integer.MAX_VALUE, buffer.remaining());
        assertEquals(0, buffer.position());
        assertEquals(Integer.MAX_VALUE, buffer.limit());
        assertTrue(buffer.hasRemaining());
    }

    @Test
    public void testCreateBufferWithGivenSize() {
        FrameWriterBuffer buffer = new FrameWriterBuffer(255);

        assertEquals(255, buffer.array().length);
        assertEquals(0, buffer.arrayOffset());
        assertEquals(Integer.MAX_VALUE, buffer.remaining());
        assertEquals(0, buffer.position());
        assertEquals(Integer.MAX_VALUE, buffer.limit());
        assertTrue(buffer.hasRemaining());
    }

    //----- Test hasRemaining ------------------------------------------------//

    @Test
    public void testHasRemaining() {
        FrameWriterBuffer buffer = new FrameWriterBuffer(10);

        for (int i = 0; i < 42; ++i) {
            assertTrue(buffer.hasRemaining());
            buffer.put((byte) 127);
            assertTrue(buffer.hasRemaining());
        }
    }

    //----- Test remaining ---------------------------------------------------//

    @Test
    public void testRemaining() {
        FrameWriterBuffer buffer = new FrameWriterBuffer(10);

        assertEquals(Integer.MAX_VALUE, buffer.remaining());
        buffer.put((byte) 127);
        assertEquals(Integer.MAX_VALUE - 1, buffer.remaining());
        buffer.put((byte) 128);
        assertEquals(Integer.MAX_VALUE - 2, buffer.remaining());
    }

    @Test
    public void testRemainingResetsWhenDataConsumed() {
        FrameWriterBuffer buffer = new FrameWriterBuffer(10);

        assertEquals(Integer.MAX_VALUE, buffer.remaining());
        buffer.put((byte) 127);
        assertEquals(Integer.MAX_VALUE - 1, buffer.remaining());
        buffer.put((byte) 128);
        assertEquals(Integer.MAX_VALUE - 2, buffer.remaining());

        ByteBuffer target = ByteBuffer.allocate(1);
        buffer.transferTo(target);
        assertEquals(Integer.MAX_VALUE - 1, buffer.remaining());
        target.clear();
        buffer.transferTo(target);
        assertEquals(Integer.MAX_VALUE, buffer.remaining());
    }

    //----- Test position handling -------------------------------------------//

    @Test
    public void testPositionWithOneArrayAppended() {
        FrameWriterBuffer buffer = new FrameWriterBuffer();

        buffer.put(new byte[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 }, 0, 10);

        assertEquals(Integer.MAX_VALUE, buffer.limit());
        assertEquals(10, buffer.position());

        buffer.position(5);
        assertEquals(5, buffer.position());

        buffer.position(6);
        assertEquals(6, buffer.position());

        buffer.position(10);
        assertEquals(10, buffer.position());

        try {
            buffer.position(11);
        } catch (IllegalArgumentException e) {
            fail("Should not throw a IllegalArgumentException");
        }
    }

    @Test
    public void testPositionMovedBeyondCurrentArraySize() {
        FrameWriterBuffer buffer = new FrameWriterBuffer(10);

        byte[] data = new byte[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 };
        buffer.put(data, 0, data.length);

        assertEquals(data.length, buffer.array().length);

        try {
            buffer.position(15);
        } catch (IllegalArgumentException e) {
            fail("Should not throw a IllegalArgumentException");
        }

        // Size should have doubled.
        assertEquals(20, buffer.array().length);

        // Check written bytes are same
        for (int i = 0; i < data.length; ++i) {
            assertEquals(data[i], buffer.array()[i]);
        }
    }

    @Test
    public void testPositionMovedBeyondDoubleTheCurrentCapacity() {
        FrameWriterBuffer buffer = new FrameWriterBuffer(10);

        byte[] data = new byte[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 };
        buffer.put(data, 0, data.length);

        assertEquals(data.length, buffer.array().length);

        try {
            buffer.position(30);
        } catch (IllegalArgumentException e) {
            fail("Should not throw a IllegalArgumentException");
        }

        // Size should have expanded to meet requested size.
        assertEquals(30, buffer.array().length);

        // Check written bytes are same
        for (int i = 0; i < data.length; ++i) {
            assertEquals(data[i], buffer.array()[i]);
        }
    }

    @Test
    public void testPositionEnforcesPreconditions() {
        FrameWriterBuffer buffer = new FrameWriterBuffer();

        try {
            buffer.position(-1);
            fail("Should throw a IllegalArgumentException");
        } catch (IllegalArgumentException e) {}
    }

    //----- Test put array ---------------------------------------------------//

    @Test
    public void testPutByteArray() {
        FrameWriterBuffer buffer = new FrameWriterBuffer(10);

        byte[] data = new byte[] { 127, (byte) 128 };

        assertEquals(0, buffer.position());
        buffer.put(data, 0, data.length);
        assertEquals(2, buffer.position());
        buffer.put(data, 0, data.length);
        assertEquals(4, buffer.position());

        assertEquals(Integer.MAX_VALUE - 4, buffer.remaining());
    }

    @Test
    public void testPutByteArrayWithZeroLengthDoesNotExpandBuffer() {
        FrameWriterBuffer buffer = new FrameWriterBuffer(0);

        byte[] data = new byte[] { 127, (byte) 128 };

        assertEquals(0, buffer.array().length);
        buffer.put(data, 0, 0);
        assertEquals(0, buffer.array().length);
    }

    @Test
    public void testPutByteArrayPastExistingCapacity() {
        FrameWriterBuffer buffer = new FrameWriterBuffer(6);

        byte[] data = new byte[] { 127, (byte) 128 };

        buffer.put(data, 0, data.length);
        buffer.put(data, 0, data.length);
        buffer.put(data, 0, data.length);

        assertEquals(6, buffer.array().length);
        assertEquals(6, buffer.position());

        // Should prompt a resize of the array
        buffer.put(data, 0, data.length);

        assertEquals(12, buffer.array().length);
        assertEquals(8, buffer.position());
    }

    @Test
    public void testPutByteArrayLargerThanDefaultExpansionSize() {
        FrameWriterBuffer buffer = new FrameWriterBuffer(6);

        byte[] data = new byte[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23};

        buffer.put(data, 0, data.length);

        // Should use the size needed instead of double current size
        assertEquals(data.length, buffer.array().length);
        assertEquals(data.length, buffer.position());
    }

    //----- Test put ByteBuffer ----------------------------------------------//

    @Test
    public void testPutByteBuffer() {
        FrameWriterBuffer buffer = new FrameWriterBuffer(10);

        byte[] data = new byte[] { 127, (byte) 128 };
        ByteBuffer byteBuffer = ByteBuffer.wrap(data);

        assertEquals(0, buffer.position());
        buffer.put(byteBuffer.duplicate());
        assertEquals(2, buffer.position());
        buffer.put(byteBuffer.duplicate());
        assertEquals(4, buffer.position());

        assertEquals(Integer.MAX_VALUE - 4, buffer.remaining());
    }

    @Test
    public void testPutByteBufferWithoutArrayAccess() {
        FrameWriterBuffer buffer = new FrameWriterBuffer(10);

        byte[] data = new byte[] { 127, (byte) 128 };
        ByteBuffer byteBuffer = ByteBuffer.wrap(data).asReadOnlyBuffer();

        assertEquals(0, buffer.position());
        buffer.put(byteBuffer.duplicate());
        assertEquals(2, buffer.position());
        buffer.put(byteBuffer.duplicate());
        assertEquals(4, buffer.position());

        assertEquals(Integer.MAX_VALUE - 4, buffer.remaining());
    }

    @Test
    public void testPutByteBufferPastExistingCapacity() {
        FrameWriterBuffer buffer = new FrameWriterBuffer(6);

        byte[] data = new byte[] { 127, (byte) 128 };
        ByteBuffer byteBuffer = ByteBuffer.wrap(data);

        buffer.put(byteBuffer.duplicate());
        buffer.put(byteBuffer.duplicate());
        buffer.put(byteBuffer.duplicate());

        assertEquals(6, buffer.array().length);
        assertEquals(6, buffer.position());

        // Should prompt a resize of the array
        buffer.put(byteBuffer);

        assertEquals(12, buffer.array().length);
        assertEquals(8, buffer.position());
    }

    @Test
    public void testPutByteBufferLargerThanDefaultExpansionSize() {
        FrameWriterBuffer buffer = new FrameWriterBuffer(6);

        byte[] data = new byte[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23};
        ByteBuffer byteBuffer = ByteBuffer.wrap(data);

        buffer.put(byteBuffer);

        // Should use the size needed instead of double current size
        assertEquals(data.length, buffer.array().length);
        assertEquals(data.length, buffer.position());
    }

    //----- Test put ReadableBuffer --------------------------------------------//

    @Test
    public void testPutReadableBuffer() {
        FrameWriterBuffer buffer = new FrameWriterBuffer(10);

        byte[] data = new byte[] { 127, (byte) 128 };
        ReadableBuffer readable = ReadableBuffer.ByteBufferReader.wrap(data);

        assertEquals(0, buffer.position());
        buffer.put(readable.duplicate());
        assertEquals(2, buffer.position());
        buffer.put(readable.duplicate());
        assertEquals(4, buffer.position());

        assertEquals(Integer.MAX_VALUE - 4, buffer.remaining());
    }

    @Test
    public void testPutReadableBufferWithoutArrayAccess() {
        FrameWriterBuffer buffer = new FrameWriterBuffer(10);

        byte[] data = new byte[] { 127, (byte) 128 };
        ByteBuffer byteBuffer = ByteBuffer.wrap(data);
        ReadableBuffer readable = ReadableBuffer.ByteBufferReader.wrap(byteBuffer.asReadOnlyBuffer());

        assertEquals(0, buffer.position());
        buffer.put(readable.duplicate());
        assertEquals(2, buffer.position());
        buffer.put(readable.duplicate());
        assertEquals(4, buffer.position());

        assertEquals(Integer.MAX_VALUE - 4, buffer.remaining());
    }

    @Test
    public void testPutReadableBufferPastExistingCapacity() {
        FrameWriterBuffer buffer = new FrameWriterBuffer(6);

        byte[] data = new byte[] { 127, (byte) 128 };
        ReadableBuffer readable = ReadableBuffer.ByteBufferReader.wrap(data);

        buffer.put(readable.duplicate());
        buffer.put(readable.duplicate());
        buffer.put(readable.duplicate());

        assertEquals(6, buffer.array().length);
        assertEquals(6, buffer.position());

        // Should prompt a resize of the array
        buffer.put(readable.duplicate());

        assertEquals(12, buffer.array().length);
        assertEquals(8, buffer.position());
    }

    @Test
    public void testPutReadableBufferLargerThanDefaultExpansionSize() {
        FrameWriterBuffer buffer = new FrameWriterBuffer(6);

        byte[] data = new byte[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23};
        ReadableBuffer readable = ReadableBuffer.ByteBufferReader.wrap(data);

        buffer.put(readable);

        // Should use the size needed instead of double current size
        assertEquals(data.length, buffer.array().length);
        assertEquals(data.length, buffer.position());
    }

    //----- Test put byte ----------------------------------------------------//

    @Test
    public void testPutByte() {
        FrameWriterBuffer buffer = new FrameWriterBuffer(10);

        assertEquals(0, buffer.position());
        buffer.put((byte) 127);
        assertEquals(1, buffer.position());
        buffer.put((byte) 128);
        assertEquals(2, buffer.position());

        assertEquals(Integer.MAX_VALUE - 2, buffer.remaining());
    }

    @Test
    public void testPutBytePastExistingCapacity() {
        FrameWriterBuffer buffer = new FrameWriterBuffer(5);

        buffer.put((byte) 127);
        buffer.put((byte) 128);
        buffer.put((byte) 127);
        buffer.put((byte) 128);
        buffer.put((byte) 128);

        assertEquals(5, buffer.array().length);
        assertEquals(5, buffer.position());

        // Should prompt a resize of the array
        buffer.put((byte) 127);

        assertEquals(10, buffer.array().length);
        assertEquals(6, buffer.position());
    }

    //----- Test put short ---------------------------------------------------//

    @Test
    public void testPutShort() {
        FrameWriterBuffer buffer = new FrameWriterBuffer(10);

        assertEquals(0, buffer.position());
        buffer.putShort((short) 127);
        assertEquals(2, buffer.position());
        buffer.putShort((short) 128);
        assertEquals(4, buffer.position());

        assertEquals(Integer.MAX_VALUE - 4, buffer.remaining());
    }

    @Test
    public void testPutShortPastExistingCapacity() {
        FrameWriterBuffer buffer = new FrameWriterBuffer(6);

        buffer.putShort((short) 128);
        buffer.putShort((short) 127);
        buffer.putShort((short) 128);

        assertEquals(6, buffer.array().length);
        assertEquals(6, buffer.position());

        // Should prompt a resize of the array
        buffer.putShort((short) 127);

        assertEquals(12, buffer.array().length);
        assertEquals(8, buffer.position());
    }

    //----- Test put int -----------------------------------------------------//

    @Test
    public void testPutInt() {
        FrameWriterBuffer buffer = new FrameWriterBuffer(10);

        assertEquals(0, buffer.position());
        buffer.putInt(127);
        assertEquals(4, buffer.position());
        buffer.putInt(128);
        assertEquals(8, buffer.position());

        assertEquals(Integer.MAX_VALUE - 8, buffer.remaining());
    }

    @Test
    public void testPutIntPastExistingCapacity() {
        FrameWriterBuffer buffer = new FrameWriterBuffer(12);

        buffer.putInt(127);
        buffer.putInt(128);
        buffer.putInt(127);

        assertEquals(12, buffer.array().length);
        assertEquals(12, buffer.position());

        // Should prompt a resize of the array
        buffer.putInt(128);

        assertEquals(24, buffer.array().length);
        assertEquals(16, buffer.position());
    }

    //----- Test put long ----------------------------------------------------//

    @Test
    public void testPutLong() {
        FrameWriterBuffer buffer = new FrameWriterBuffer(20);

        assertEquals(0, buffer.position());
        buffer.putLong(127);
        assertEquals(8, buffer.position());
        buffer.putLong(128);
        assertEquals(16, buffer.position());

        assertEquals(Integer.MAX_VALUE - 16, buffer.remaining());
    }

    @Test
    public void testPutLongPastExistingCapacity() {
        FrameWriterBuffer buffer = new FrameWriterBuffer(24);

        buffer.putLong(127);
        buffer.putLong(128);
        buffer.putLong(127);

        assertEquals(24, buffer.array().length);
        assertEquals(24, buffer.position());

        // Should prompt a resize of the array
        buffer.putLong(128);

        assertEquals(48, buffer.array().length);
        assertEquals(32, buffer.position());
    }

    //----- Test put float ---------------------------------------------------//

    @Test
    public void testPutFloat() {
        FrameWriterBuffer buffer = new FrameWriterBuffer(8);

        assertEquals(0, buffer.position());
        buffer.putFloat(127);
        assertEquals(4, buffer.position());
        buffer.putFloat(128);
        assertEquals(8, buffer.position());

        assertEquals(Integer.MAX_VALUE - 8, buffer.remaining());
    }

    @Test
    public void testPutFloatPastExistingCapacity() {
        FrameWriterBuffer buffer = new FrameWriterBuffer(12);

        buffer.putFloat(127);
        buffer.putFloat(128);
        buffer.putFloat(127);

        assertEquals(12, buffer.array().length);
        assertEquals(12, buffer.position());

        // Should prompt a resize of the array
        buffer.putFloat(128);

        assertEquals(24, buffer.array().length);
        assertEquals(16, buffer.position());
    }

    //----- Test put double --------------------------------------------------//

    @Test
    public void testPutDouble() {
        FrameWriterBuffer buffer = new FrameWriterBuffer(20);

        assertEquals(0, buffer.position());
        buffer.putDouble(127);
        assertEquals(8, buffer.position());
        buffer.putDouble(128);
        assertEquals(16, buffer.position());

        assertEquals(Integer.MAX_VALUE - 16, buffer.remaining());
    }

    @Test
    public void testPutDoublePastExistingCapacity() {
        FrameWriterBuffer buffer = new FrameWriterBuffer(24);

        buffer.putDouble(127);
        buffer.putDouble(128);
        buffer.putDouble(127);

        assertEquals(24, buffer.array().length);
        assertEquals(24, buffer.position());

        // Should prompt a resize of the array
        buffer.putDouble(128);

        assertEquals(48, buffer.array().length);
        assertEquals(32, buffer.position());
    }

    //----- Test transferTo --------------------------------------------------//

    @Test
    public void testTrasnferHandlesZeroSizedReadRequest() {
        FrameWriterBuffer buffer = new FrameWriterBuffer(6);

        byte[] data = new byte[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23};
        ByteBuffer target = ByteBuffer.allocate(0);

        buffer.put(data, 0, data.length);

        // Should use the size needed instead of double current size
        assertEquals(data.length, buffer.array().length);
        assertEquals(data.length, buffer.position());

        buffer.transferTo(target);

        assertEquals(data.length, buffer.position());

        // Should now be nothing to transfer
        target.clear();

        buffer.transferTo(target);

        assertEquals(data.length, buffer.position());
        assertEquals(0, target.position());
    }

    @Test
    public void testTrasnferFullBufferToTarget() {
        FrameWriterBuffer buffer = new FrameWriterBuffer(6);

        byte[] data = new byte[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23};
        ByteBuffer target = ByteBuffer.allocate(data.length);

        buffer.put(data, 0, data.length);

        // Should use the size needed instead of double current size
        assertEquals(data.length, buffer.array().length);
        assertEquals(data.length, buffer.position());

        buffer.transferTo(target);

        assertEquals(0, buffer.position());

        assertArrayEquals(data, target.array());
    }

    @Test
    public void testTrasnferFullBufferToTargetWithoutArrayAccess() {
        FrameWriterBuffer buffer = new FrameWriterBuffer(6);

        byte[] data = new byte[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23};
        ByteBuffer target = ByteBuffer.allocateDirect(data.length);

        buffer.put(data, 0, data.length);

        // Should use the size needed instead of double current size
        assertEquals(data.length, buffer.array().length);
        assertEquals(data.length, buffer.position());

        buffer.transferTo(target);

        assertEquals(0, buffer.position());

        // Check contents in target are correct which requires getting the data out first.
        target.flip();
        byte[] targetPayload = new byte[target.remaining()];
        target.get(targetPayload);
        assertArrayEquals(data, targetPayload);
    }

    @Test
    public void testTrasnferPartialBufferToTarget() {
        FrameWriterBuffer buffer = new FrameWriterBuffer(6);

        byte[] data = new byte[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23};
        ByteBuffer target = ByteBuffer.allocate(data.length);

        buffer.put(data, 0, data.length);

        // Should use the size needed instead of double current size
        assertEquals(data.length, buffer.array().length);
        assertEquals(data.length, buffer.position());

        // First Half
        target.limit(data.length / 2);
        buffer.transferTo(target);

        // The buffer should have compacted
        assertEquals(data.length / 2, buffer.position());

        // Second half
        target.limit(target.capacity());
        target.position(data.length / 2);
        buffer.transferTo(target);

        // Buffer contents should have been consumed
        assertEquals(0, buffer.arrayOffset());
        assertEquals(0, buffer.position());

        assertArrayEquals(data, target.array());
    }

    @Test
    public void testTrasnferPartialBufferThenRequestMoreThanIsRemaining() {
        FrameWriterBuffer buffer = new FrameWriterBuffer(10);

        byte[] data = new byte[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};

        buffer.put(data, 0, data.length);

        // Should use the size needed instead of double current size
        assertEquals(data.length, buffer.array().length);
        assertEquals(data.length, buffer.position());

        // First Half transfered which should lead to compaction
        ByteBuffer target = ByteBuffer.allocate(5);
        buffer.transferTo(target);
        assertEquals(data.length / 2, buffer.position());

        // Now the buffer should compact
        buffer.ensureRemaining(10);
        assertEquals(data.length / 2, buffer.position());

        // Second half
        target = ByteBuffer.allocate(5);
        buffer.transferTo(target);

        // Buffer contents should have been consumed
        assertEquals(0, buffer.position());

        assertArrayEquals(new byte[] {5, 6, 7, 8, 9}, target.array());
    }
}